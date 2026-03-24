package org.purpurmc.purpur.network;

import javax.net.ssl.*;
import java.io.*;
import java.net.Socket;
import java.security.KeyStore;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataClient
 *
 * Maintains a persistent TCP (optionally TLS) connection to the master server.
 *
 * FIX FOR TIMEOUT BUG:
 * ─────────────────────
 * The original code used a single readTimeoutMs for ALL operations (default 3000 ms).
 * Chunk NBT payloads can be 8–64 KB compressed. At even 10 MB/s that's fine, but
 * the synchronized(dos) write + network flush + server processing + synchronized
 * response read all have to fit inside 3000 ms while other threads may be competing
 * for the same dos lock.
 *
 * Fix: two separate timeouts:
 *   • shortTimeoutMs  – for PING, GET, DELETE, AUTH (small, fast)
 *   • chunkTimeoutMs  – for PUT of chunk/* keys (large payload, needs more time)
 *
 * Additionally, all pending futures are keyed to their request ID.  If two threads
 * both try to PUT the same key (immediate virtual thread + flush thread retry), only
 * the first one actually writes to the wire; the second sees the key is in-flight and
 * waits for its future to complete rather than firing a duplicate write.
 *
 * Reconnection: exponential backoff capped at 30 seconds.
 */
public class RemoteDataClient {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataClient");

    // ── Singleton ────────────────────────────────────────────────────────────
    private static RemoteDataClient INSTANCE;
    public static RemoteDataClient get()                        { return INSTANCE; }
    public static RemoteDataClient init(RemoteDataConfig cfg)   { INSTANCE = new RemoteDataClient(cfg); return INSTANCE; }

    // ── Fields ───────────────────────────────────────────────────────────────
    private final RemoteDataConfig config;
    private final HmacHelper       hmac;
    private final AtomicInteger    requestIdGen = new AtomicInteger(1);

    /** Pending request futures: requestId → future */
    private final ConcurrentHashMap<Integer, CompletableFuture<RemoteDataPacket>> pending = new ConcurrentHashMap<>();

    /**
     * In-flight PUT tracker: chunkKey → future that completes when the PUT
     * finishes (success or failure).  Prevents the flush-thread from firing a
     * duplicate PUT while an immediate virtual-thread push is still in progress.
     */
    private final ConcurrentHashMap<String, CompletableFuture<Boolean>> inFlightPuts = new ConcurrentHashMap<>();

    private volatile Socket           socket;
    private volatile DataInputStream  dis;
    private volatile DataOutputStream dos;
    private volatile boolean          connected = false;
    private volatile boolean          running   = false;

    private Thread readerThread;
    private Thread reconnectThread;

    // ── Constructor ──────────────────────────────────────────────────────────
    public RemoteDataClient(RemoteDataConfig config) {
        this.config = config;
        this.hmac   = new HmacHelper(config.secretKey);
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────
    public void start() {
        running         = true;
        reconnectThread = new Thread(this::reconnectLoop, "RemoteData-Reconnect");
        reconnectThread.setDaemon(true);
        reconnectThread.start();
        LOGGER.info("[RemoteData] Client started. Target: " + config.masterHost + ":" + config.masterPort);
    }

    public void stop() {
        running = false;
        if (reconnectThread != null) reconnectThread.interrupt();
        if (readerThread    != null) readerThread.interrupt();
        closeSocket();
        LOGGER.info("[RemoteData] Client stopped.");
    }

    public boolean isConnected() { return connected; }

    // ── Public API ───────────────────────────────────────────────────────────

    /** Fetch a value from the master.  Returns null if not found or on error. */
    public byte[] get(String key) {
        if (!connected) return null;
        try {
            RemoteDataPacket resp = sendAndWait(
                new RemoteDataPacket(RemoteDataPacket.OpCode.GET, nextId(), key, null),
                config.shortTimeoutMs);
            if (resp.opCode == RemoteDataPacket.OpCode.DATA) return resp.data;
            return null;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] GET failed for key: " + key, e);
            return null;
        }
    }

    /**
     * Store a value on the master.
     *
     * FIX: chunk/* keys use chunkPutTimeoutMs (default 30 s) instead of
     * shortTimeoutMs (default 5 s) so large compressed payloads never time out.
     *
     * FIX: deduplicates concurrent PUTs for the same key using inFlightPuts.
     * If a virtual thread is already pushing key X, any second caller (flush
     * thread) will await the first future instead of sending a duplicate packet.
     */
    public boolean put(String key, byte[] data) {
        if (!connected) return false;

        long timeoutMs = key.startsWith("chunk/") ? config.chunkPutTimeoutMs : config.shortTimeoutMs;

        // ── Deduplication: if a PUT for this key is already in flight, wait for it ──
        CompletableFuture<Boolean> myFuture  = new CompletableFuture<>();
        CompletableFuture<Boolean> existing  = inFlightPuts.putIfAbsent(key, myFuture);
        if (existing != null) {
            // Another thread is already sending this key — just piggyback on its result.
            try {
                return existing.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                return false;
            }
        }

        // We won the race — we are responsible for this PUT.
        try {
            RemoteDataPacket resp = sendAndWait(
                new RemoteDataPacket(RemoteDataPacket.OpCode.PUT, nextId(), key, data),
                timeoutMs);
            boolean ok = resp.opCode == RemoteDataPacket.OpCode.OK;
            myFuture.complete(ok);
            return ok;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] PUT failed for key: " + key, e);
            myFuture.complete(false);
            return false;
        } finally {
            inFlightPuts.remove(key, myFuture);
        }
    }

    /** Delete a key from the master.  Returns true on success. */
    public boolean delete(String key) {
        if (!connected) return false;
        try {
            RemoteDataPacket resp = sendAndWait(
                new RemoteDataPacket(RemoteDataPacket.OpCode.DELETE, nextId(), key, null),
                config.shortTimeoutMs);
            return resp.opCode == RemoteDataPacket.OpCode.OK;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] DELETE failed for key: " + key, e);
            return false;
        }
    }

    /** Ping the master.  Returns round-trip latency in ms, or -1 on failure. */
    public long ping() {
        if (!connected) return -1;
        try {
            long t    = System.currentTimeMillis();
            RemoteDataPacket resp = sendAndWait(
                new RemoteDataPacket(RemoteDataPacket.OpCode.PING, nextId(), "", null),
                config.shortTimeoutMs);
            if (resp.opCode == RemoteDataPacket.OpCode.PONG) return System.currentTimeMillis() - t;
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "[RemoteData] Ping failed", e);
        }
        return -1;
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private int nextId() {
        int id = requestIdGen.getAndIncrement();
        if (id == 0) id = requestIdGen.getAndIncrement();
        return id;
    }

    /**
     * Write a packet to the wire and block until the matching response arrives,
     * or until timeoutMs elapses.
     *
     * FIX: timeoutMs is now a parameter so callers can supply the right budget
     * for the operation size (small vs. large chunk payload).
     */
    private RemoteDataPacket sendAndWait(RemoteDataPacket pkt, long timeoutMs) throws Exception {
        int id = pkt.requestId;
        CompletableFuture<RemoteDataPacket> future = new CompletableFuture<>();
        pending.put(id, future);
        try {
            synchronized (dos) {
                pkt.writeTo(dos, hmac, config.compressionEnabled);
            }
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            pending.remove(id);
            throw new IOException("Request timed out after " + timeoutMs + "ms: "
                    + pkt.opCode + " key=" + pkt.key);
        } catch (Exception e) {
            pending.remove(id);
            throw e;
        }
    }

    // ── Reader thread ────────────────────────────────────────────────────────

    private void startReader() {
        readerThread = new Thread(() -> {
            while (running && connected) {
                try {
                    RemoteDataPacket pkt = RemoteDataPacket.readFrom(dis, hmac);
                    // requestId == 0 → unsolicited server push
                    if (pkt.requestId == 0) {
                        handleServerPush(pkt);
                        continue;
                    }
                    CompletableFuture<RemoteDataPacket> future = pending.remove(pkt.requestId);
                    if (future != null) {
                        future.complete(pkt);
                    } else {
                        LOGGER.fine("[RemoteData] Unsolicited packet with unknown requestId: " + pkt.requestId);
                    }
                } catch (IOException e) {
                    if (running) {
                        LOGGER.warning("[RemoteData] Reader error, reconnecting: " + e.getMessage());
                        handleDisconnect();
                    }
                    break;
                }
            }
        }, "RemoteData-Reader");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    /**
     * Handle an unsolicited packet pushed from the master server.
     * CHUNK_PUSH: master is broadcasting new chunk data saved by another server.
     */
    private void handleServerPush(RemoteDataPacket pkt) {
        if (pkt.opCode == RemoteDataPacket.OpCode.CHUNK_PUSH) {
            String key  = pkt.key;
            byte[] data = pkt.data;

            if (data == null || data.length == 0) {
                RemoteDataCache cache = RemoteDataCache.get();
                if (cache != null) cache.invalidate(key);
                LOGGER.fine("[RemoteData] Received CHUNK_PUSH invalidate for key=" + key);
                return;
            }

            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) cache.putClean(key, data);

            try {
                RemoteChunkDataHandler.applyPush(key, data);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "[RemoteData] Failed to apply live chunk push for key=" + key, e);
            }
            LOGGER.fine("[RemoteData] Applied CHUNK_PUSH key=" + key + " (" + data.length + " bytes)");
        }
    }

    // ── Reconnect loop ───────────────────────────────────────────────────────

    private void reconnectLoop() {
        long backoffMs = 1000;
        while (running) {
            if (!connected) {
                try {
                    connectToMaster();
                    backoffMs = 1000;
                } catch (Exception e) {
                    LOGGER.warning("[RemoteData] Could not connect to master ("
                            + e.getMessage() + "). Retrying in " + (backoffMs / 1000) + "s...");
                    try { Thread.sleep(backoffMs); } catch (InterruptedException ie) { break; }
                    backoffMs = Math.min(backoffMs * 2, 30_000);
                }
            } else {
                try { Thread.sleep(2000); } catch (InterruptedException e) { break; }
            }
        }
    }

    private void connectToMaster() throws IOException {
        closeSocket();

        Socket s;
        if (config.useTLS) {
            s = buildTLSSocket();
        } else {
            s = new Socket();
            s.connect(new java.net.InetSocketAddress(config.masterHost, config.masterPort),
                    config.connectTimeoutMs);
        }
        s.setTcpNoDelay(true);
        s.setSoTimeout(0);   // no read timeout on the socket itself — we use CompletableFuture timeouts
        s.setKeepAlive(true);
        // Increase OS send buffer so large chunk payloads don't stall in the kernel
        s.setSendBufferSize(256 * 1024);
        s.setReceiveBufferSize(256 * 1024);

        socket = s;
        dis    = new DataInputStream(new BufferedInputStream(s.getInputStream(),   128 * 1024));
        dos    = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 128 * 1024));

        // Authenticate
        int authId = nextId();
        CompletableFuture<RemoteDataPacket> authFuture = new CompletableFuture<>();
        pending.put(authId, authFuture);

        connected = true;
        startReader();  // start reader before sending AUTH so we can receive AUTH_OK

        byte[] keyBytes = config.secretKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        RemoteDataPacket authPkt = new RemoteDataPacket(
                RemoteDataPacket.OpCode.AUTH, authId, "auth", keyBytes);
        synchronized (dos) {
            authPkt.writeTo(dos, hmac, false);
        }

        try {
            RemoteDataPacket authResp = authFuture.get(config.connectTimeoutMs, TimeUnit.MILLISECONDS);
            if (authResp.opCode != RemoteDataPacket.OpCode.AUTH_OK) {
                connected = false;
                closeSocket();
                throw new IOException("Authentication rejected by master");
            }
        } catch (TimeoutException e) {
            connected = false;
            closeSocket();
            throw new IOException("Auth timeout");
        } catch (ExecutionException | InterruptedException e) {
            connected = false;
            closeSocket();
            throw new IOException("Auth failed: " + e.getMessage());
        }

        LOGGER.info("[RemoteData] Connected and authenticated to master "
                + config.masterHost + ":" + config.masterPort);
    }

    private void handleDisconnect() {
        connected = false;
        // Complete all waiting futures exceptionally so callers unblock immediately
        pending.forEach((id, f) -> f.completeExceptionally(new IOException("Disconnected from master")));
        pending.clear();
        inFlightPuts.forEach((k, f) -> f.complete(false));
        inFlightPuts.clear();
        closeSocket();
    }

    private void closeSocket() {
        Socket s = socket;
        if (s != null && !s.isClosed()) {
            try { s.close(); } catch (IOException ignored) {}
        }
        socket    = null;
        connected = false;
    }

    // ── TLS ──────────────────────────────────────────────────────────────────

    private Socket buildTLSSocket() throws IOException {
        try {
            KeyStore ks = KeyStore.getInstance("JKS");
            try (FileInputStream fis = new FileInputStream(config.tlsKeystorePath)) {
                ks.load(fis, config.tlsKeystorePassword.toCharArray());
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            SSLContext ctx = SSLContext.getInstance("TLSv1.3");
            ctx.init(null, tmf.getTrustManagers(), null);
            SSLSocket ss = (SSLSocket) ctx.getSocketFactory().createSocket();
            ss.connect(new java.net.InetSocketAddress(config.masterHost, config.masterPort),
                    config.connectTimeoutMs);
            ss.startHandshake();
            return ss;
        } catch (Exception e) {
            throw new IOException("TLS setup failed: " + e.getMessage(), e);
        }
    }
}
