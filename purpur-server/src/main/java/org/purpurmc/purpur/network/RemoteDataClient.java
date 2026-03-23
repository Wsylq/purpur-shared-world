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
 * All operations are synchronous from the caller's perspective but use a
 * dedicated reader thread and a future-map to handle pipelined responses.
 *
 * Real-time chunk sync:
 *   The reader thread handles CHUNK_PUSH packets sent unsolicited by the master.
 *   When received, the chunk data is immediately written into the local cache
 *   (putClean) so the next chunk load on this server gets the updated data.
 *   If the chunk is currently loaded in memory, it is also refreshed via
 *   RemoteChunkDataHandler.applyPush().
 *
 * Reconnection: exponential backoff with cap at 30 seconds.
 */
public class RemoteDataClient {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataClient");

    // ── Singleton ─────────────────────────────────────────────────────────────
    private static RemoteDataClient INSTANCE;

    public static RemoteDataClient get() { return INSTANCE; }

    public static RemoteDataClient init(RemoteDataConfig config) {
        INSTANCE = new RemoteDataClient(config);
        return INSTANCE;
    }

    // ── Fields ────────────────────────────────────────────────────────────────
    private final RemoteDataConfig config;
    private final HmacHelper hmac;
    private final AtomicInteger requestIdGen = new AtomicInteger(1);

    /** Pending requests: requestId → CompletableFuture */
    private final ConcurrentHashMap<Integer, CompletableFuture<RemoteDataPacket>> pending = new ConcurrentHashMap<>();

    private volatile Socket socket;
    private volatile DataInputStream dis;
    private volatile DataOutputStream dos;
    private volatile boolean connected = false;
    private volatile boolean running = false;
    private Thread readerThread;
    private Thread reconnectThread;

    // ── Constructor ───────────────────────────────────────────────────────────
    public RemoteDataClient(RemoteDataConfig config) {
        this.config = config;
        this.hmac = new HmacHelper(config.secretKey);
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────
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
        if (readerThread != null) readerThread.interrupt();
        closeSocket();
        LOGGER.info("[RemoteData] Client stopped.");
    }

    public boolean isConnected() { return connected; }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Fetch a value from the master. Returns null if not found or on error.
     */
    public byte[] get(String key) {
        if (!connected) return null;
        try {
            RemoteDataPacket resp = sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.GET, nextId(), key, null));
            if (resp.opCode == RemoteDataPacket.OpCode.DATA) return resp.data;
            return null;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] GET failed for key: " + key, e);
            return null;
        }
    }

    /**
     * Store a value on the master. Returns true on success.
     */
    public boolean put(String key, byte[] data) {
        if (!connected) return false;
        try {
            RemoteDataPacket resp = sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.PUT, nextId(), key, data));
            return resp.opCode == RemoteDataPacket.OpCode.OK;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] PUT failed for key: " + key, e);
            return false;
        }
    }

    /**
     * Delete a key from the master. Returns true on success.
     */
    public boolean delete(String key) {
        if (!connected) return false;
        try {
            RemoteDataPacket resp = sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.DELETE, nextId(), key, null));
            return resp.opCode == RemoteDataPacket.OpCode.OK;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] DELETE failed for key: " + key, e);
            return false;
        }
    }

    /**
     * Ping the master. Returns round-trip latency in ms, or -1 on failure.
     */
    public long ping() {
        if (!connected) return -1;
        try {
            long t = System.currentTimeMillis();
            RemoteDataPacket resp = sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.PING, nextId(), "", null));
            if (resp.opCode == RemoteDataPacket.OpCode.PONG) {
                return System.currentTimeMillis() - t;
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "[RemoteData] Ping failed", e);
        }
        return -1;
    }

    // ── Internal ──────────────────────────────────────────────────────────────
    private int nextId() {
        // Reserve 0 for server-push packets; start from 1
        int id = requestIdGen.getAndIncrement();
        if (id == 0) id = requestIdGen.getAndIncrement();
        return id;
    }

    private RemoteDataPacket sendAndWait(RemoteDataPacket pkt) throws Exception {
        int id = pkt.requestId;
        CompletableFuture<RemoteDataPacket> future = new CompletableFuture<>();
        pending.put(id, future);
        try {
            synchronized (dos) {
                pkt.writeTo(dos, hmac, config.compressionEnabled);
            }
            return future.get(config.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            pending.remove(id);
            throw new IOException("Request timed out: " + pkt.opCode + " key=" + pkt.key);
        } catch (Exception e) {
            pending.remove(id);
            throw e;
        }
    }

    // ── Reader thread ─────────────────────────────────────────────────────────
    private void startReader() {
        readerThread = new Thread(() -> {
            while (running && connected) {
                try {
                    RemoteDataPacket pkt = RemoteDataPacket.readFrom(dis, hmac);

                    // ── Handle unsolicited server-push packets (requestId == 0) ──
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
     * Currently handles CHUNK_PUSH for real-time shared world sync.
     */
    private void handleServerPush(RemoteDataPacket pkt) {
        if (pkt.opCode == RemoteDataPacket.OpCode.CHUNK_PUSH) {
            String key = pkt.key;
            byte[] data = pkt.data;

            if (data == null || data.length == 0) {
                // Empty push = invalidate only
                RemoteDataCache cache = RemoteDataCache.get();
                if (cache != null) cache.invalidate(key);
                LOGGER.fine("[RemoteData] Received CHUNK_PUSH invalidate for key=" + key);
                return;
            }

            // Write into local cache (clean = no need to send back to master)
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.putClean(key, data);
            }

            // If this chunk is currently loaded in the server's world, apply the update live
            try {
                RemoteChunkDataHandler.applyPush(key, data);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "[RemoteData] Failed to apply live chunk push for key=" + key, e);
            }

            LOGGER.fine("[RemoteData] Applied CHUNK_PUSH key=" + key + " (" + data.length + " bytes)");
        }
    }

    // ── Reconnect loop ────────────────────────────────────────────────────────
    private void reconnectLoop() {
        long backoffMs = 1000;
        while (running) {
            if (!connected) {
                try {
                    connectToMaster();
                    backoffMs = 1000; // reset on success
                } catch (Exception e) {
                    LOGGER.warning("[RemoteData] Could not connect to master ("
                            + e.getMessage() + "). Retrying in " + (backoffMs / 1000) + "s...");
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        break;
                    }
                    backoffMs = Math.min(backoffMs * 2, 30_000);
                }
            } else {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    break;
                }
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
            s.connect(
                    new java.net.InetSocketAddress(config.masterHost, config.masterPort),
                    config.connectTimeoutMs);
        }
        s.setTcpNoDelay(true);
        s.setSoTimeout(0);
        s.setKeepAlive(true);

        socket = s;
        dis = new DataInputStream(new BufferedInputStream(s.getInputStream(), 65536));
        dos = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 65536));

        // Authenticate
        int authId = nextId();
        CompletableFuture<RemoteDataPacket> authFuture = new CompletableFuture<>();
        pending.put(authId, authFuture);

        // Start reader first so we get the AUTH_OK
        connected = true;
        startReader();

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
        pending.forEach((id, f) -> f.completeExceptionally(new IOException("Disconnected from master")));
        pending.clear();
        closeSocket();
    }

    private void closeSocket() {
        Socket s = socket;
        if (s != null && !s.isClosed()) {
            try { s.close(); } catch (IOException ignored) {}
        }
        socket = null;
        connected = false;
    }

    // ── TLS ───────────────────────────────────────────────────────────────────
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
            SSLSocket s = (SSLSocket) ctx.getSocketFactory().createSocket();
            s.connect(new java.net.InetSocketAddress(config.masterHost, config.masterPort), config.connectTimeoutMs);
            s.startHandshake();
            return s;
        } catch (Exception e) {
            throw new IOException("TLS setup failed: " + e.getMessage(), e);
        }
    }
}
