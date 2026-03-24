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
 * RemoteDataClient – connects to the master server.
 *
 * v2 changes:
 *   • sendBlockUpdate(world, x, y, z, blockStateString) – fires a BLOCK_UPDATE
 *     packet to master, which master immediately re-broadcasts as BLOCK_PUSH to
 *     all other servers.  This is the real-time block sync path.
 *   • handleServerPush() now handles both CHUNK_PUSH and BLOCK_PUSH.
 *   • BLOCK_PUSH is handled by RemoteBlockHandler.applyPush().
 *   • requestId == 0 packets (server pushes) are detected BEFORE the pending-
 *     futures lookup and dispatched directly — they were previously silently dropped.
 */
public class RemoteDataClient {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataClient");

    // ── Singleton ────────────────────────────────────────────────────────────
    private static RemoteDataClient INSTANCE;
    public static RemoteDataClient get()              { return INSTANCE; }
    public static RemoteDataClient init(RemoteDataConfig cfg) {
        INSTANCE = new RemoteDataClient(cfg);
        return INSTANCE;
    }

    // ── Fields ───────────────────────────────────────────────────────────────
    private final RemoteDataConfig config;
    private final HmacHelper       hmac;
    private final AtomicInteger    requestIdGen = new AtomicInteger(1);

    private final ConcurrentHashMap<Integer, CompletableFuture<RemoteDataPacket>> pending      = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,  CompletableFuture<Boolean>>          inFlightPuts = new ConcurrentHashMap<>();

    private volatile Socket           socket;
    private volatile DataInputStream  dis;
    private volatile DataOutputStream dos;
    private volatile boolean connected = false;
    private volatile boolean running   = false;

    private Thread readerThread;
    private Thread reconnectThread;

    // ── Constructor ──────────────────────────────────────────────────────────
    public RemoteDataClient(RemoteDataConfig config) {
        this.config = config;
        this.hmac   = new HmacHelper(config.secretKey);
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────
    public void start() {
        running = true;
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

    public byte[] get(String key) {
        if (!connected) return null;
        try {
            RemoteDataPacket resp = sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.GET, nextId(), key, null),
                    config.shortTimeoutMs);
            return resp.opCode == RemoteDataPacket.OpCode.DATA ? resp.data : null;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] GET failed: " + key, e);
            return null;
        }
    }

    public boolean put(String key, byte[] data) {
        if (!connected) return false;
        long timeoutMs = key.startsWith("chunk/") ? config.chunkPutTimeoutMs : config.shortTimeoutMs;

        // Deduplicate concurrent PUTs for the same key
        CompletableFuture<Boolean> mine = new CompletableFuture<>();
        CompletableFuture<Boolean> existing = inFlightPuts.putIfAbsent(key, mine);
        if (existing != null) {
            try { return existing.get(timeoutMs, TimeUnit.MILLISECONDS); }
            catch (Exception e) { return false; }
        }
        try {
            RemoteDataPacket resp = sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.PUT, nextId(), key, data),
                    timeoutMs);
            boolean ok = resp.opCode == RemoteDataPacket.OpCode.OK;
            mine.complete(ok);
            return ok;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] PUT failed: " + key, e);
            mine.complete(false);
            return false;
        } finally {
            inFlightPuts.remove(key, mine);
        }
    }

    /**
     * Send a real-time single-block change to the master.
     * Master will immediately broadcast a BLOCK_PUSH to all other connected servers.
     *
     * key    = "world/x/y/z"
     * data   = blockStateString bytes (UTF-8), e.g. "minecraft:stone"
     *
     * Fire-and-forget on a virtual thread — does not block the calling (main) thread.
     */
    public void sendBlockUpdate(String blockKey, byte[] blockStateBytes) {
        if (!connected) return;
        Thread.ofVirtual().name("RemoteData-BlockUpdate-" + blockKey).start(() -> {
            try {
                sendAndWait(
                        new RemoteDataPacket(RemoteDataPacket.OpCode.BLOCK_UPDATE, nextId(), blockKey, blockStateBytes),
                        config.shortTimeoutMs);
            } catch (Exception e) {
                LOGGER.log(Level.FINE, "[RemoteData] BLOCK_UPDATE failed: " + blockKey, e);
            }
        });
    }

    public boolean delete(String key) {
        if (!connected) return false;
        try {
            RemoteDataPacket resp = sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.DELETE, nextId(), key, null),
                    config.shortTimeoutMs);
            return resp.opCode == RemoteDataPacket.OpCode.OK;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] DELETE failed: " + key, e);
            return false;
        }
    }

    public long ping() {
        if (!connected) return -1;
        try {
            long t = System.currentTimeMillis();
            RemoteDataPacket resp = sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.PING, nextId(), "", null),
                    config.shortTimeoutMs);
            return resp.opCode == RemoteDataPacket.OpCode.PONG ? System.currentTimeMillis() - t : -1;
        } catch (Exception e) { return -1; }
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private int nextId() {
        int id = requestIdGen.getAndIncrement();
        if (id == 0) id = requestIdGen.getAndIncrement();
        return id;
    }

    private RemoteDataPacket sendAndWait(RemoteDataPacket pkt, long timeoutMs) throws Exception {
        int id = pkt.requestId;
        CompletableFuture<RemoteDataPacket> future = new CompletableFuture<>();
        pending.put(id, future);
        try {
            synchronized (dos) {
                pkt.writeTo(dos, hmac, config.compressionEnabled);
                dos.flush();
            }
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            pending.remove(id);
            throw new IOException("Timed out after " + timeoutMs + "ms: " + pkt.opCode + " key=" + pkt.key);
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

                    // requestId == 0 → unsolicited server push (CHUNK_PUSH / BLOCK_PUSH)
                    // MUST be handled BEFORE the pending lookup — they have no matching request.
                    if (pkt.requestId == 0) {
                        handleServerPush(pkt);
                        continue;
                    }

                    CompletableFuture<RemoteDataPacket> future = pending.remove(pkt.requestId);
                    if (future != null) {
                        future.complete(pkt);
                    } else {
                        LOGGER.fine("[RemoteData] No pending future for requestId=" + pkt.requestId);
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
     * Handle an unsolicited packet pushed from master.
     *
     * BLOCK_PUSH  – apply a single block change immediately on the main thread.
     *               This is the primary real-time sync path.
     * CHUNK_PUSH  – update cache and unload chunk so next load picks up fresh data.
     *               This is the fallback / login-correctness path.
     */
    private void handleServerPush(RemoteDataPacket pkt) {
        switch (pkt.opCode) {
            case BLOCK_PUSH -> {
                // key = "world/x/y/z", data = blockStateString bytes
                LOGGER.fine("[RemoteData] Received BLOCK_PUSH key=" + pkt.key);
                try {
                    RemoteBlockHandler.applyPush(pkt.key, pkt.data);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "[RemoteData] BLOCK_PUSH apply failed for key=" + pkt.key, e);
                }
            }
            case CHUNK_PUSH -> {
                String key = pkt.key;
                if (pkt.data == null || pkt.data.length == 0) {
                    RemoteDataCache cache = RemoteDataCache.get();
                    if (cache != null) cache.invalidate(key);
                    return;
                }
                RemoteDataCache cache = RemoteDataCache.get();
                if (cache != null) cache.putClean(key, pkt.data);
                try {
                    RemoteChunkDataHandler.applyPush(key, pkt.data);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "[RemoteData] CHUNK_PUSH apply failed for key=" + key, e);
                }
                LOGGER.fine("[RemoteData] Applied CHUNK_PUSH key=" + key + " (" + pkt.data.length + " bytes)");
            }
            default -> LOGGER.fine("[RemoteData] Unknown server push opCode=" + pkt.opCode);
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
                    LOGGER.warning("[RemoteData] Could not connect (" + e.getMessage() + "). Retrying in " + (backoffMs/1000) + "s...");
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
        Socket s = config.useTLS ? buildTLSSocket() : new Socket();
        if (!config.useTLS) s.connect(new java.net.InetSocketAddress(config.masterHost, config.masterPort), config.connectTimeoutMs);
        s.setTcpNoDelay(true);
        s.setSoTimeout(0);
        s.setKeepAlive(true);
        s.setSendBufferSize(256 * 1024);
        s.setReceiveBufferSize(256 * 1024);

        socket = s;
        dis = new DataInputStream (new BufferedInputStream (s.getInputStream(),  128 * 1024));
        dos = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 128 * 1024));

        int authId = nextId();
        CompletableFuture<RemoteDataPacket> authFuture = new CompletableFuture<>();
        pending.put(authId, authFuture);
        connected = true;
        startReader();

        RemoteDataPacket authPkt = new RemoteDataPacket(
                RemoteDataPacket.OpCode.AUTH, authId, "auth",
                config.secretKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        synchronized (dos) {
            authPkt.writeTo(dos, hmac, false);
            dos.flush();
        }

        try {
            RemoteDataPacket authResp = authFuture.get(config.connectTimeoutMs, TimeUnit.MILLISECONDS);
            if (authResp.opCode != RemoteDataPacket.OpCode.AUTH_OK) {
                connected = false; closeSocket();
                throw new IOException("Authentication rejected by master");
            }
        } catch (TimeoutException e) {
            connected = false; closeSocket();
            throw new IOException("Auth timeout");
        } catch (ExecutionException | InterruptedException e) {
            connected = false; closeSocket();
            throw new IOException("Auth failed: " + e.getMessage());
        }
        LOGGER.info("[RemoteData] Connected and authenticated to master " + config.masterHost + ":" + config.masterPort);
    }

    private void handleDisconnect() {
        connected = false;
        pending.forEach((id, f) -> f.completeExceptionally(new IOException("Disconnected")));
        pending.clear();
        inFlightPuts.forEach((k, f) -> f.complete(false));
        inFlightPuts.clear();
        closeSocket();
    }

    private void closeSocket() {
        Socket s = socket;
        if (s != null && !s.isClosed()) { try { s.close(); } catch (IOException ignored) {} }
        socket = null;
        connected = false;
    }

    // ── TLS ──────────────────────────────────────────────────────────────────
    private Socket buildTLSSocket() throws IOException {
        try {
            KeyStore ks = KeyStore.getInstance("JKS");
            try (FileInputStream fis = new FileInputStream(config.tlsKeystorePath)) {
                ks.load(fis, config.tlsKeystorePassword.toCharArray());
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            SSLContext ctx = SSLContext.getInstance("TLSv1.3");
            ctx.init(null, tmf.getTrustManagers(), null);
            SSLSocket ss = (SSLSocket) ctx.getSocketFactory().createSocket();
            ss.connect(new java.net.InetSocketAddress(config.masterHost, config.masterPort), config.connectTimeoutMs);
            ss.startHandshake();
            return ss;
        } catch (Exception e) {
            throw new IOException("TLS setup failed: " + e.getMessage(), e);
        }
    }
}
