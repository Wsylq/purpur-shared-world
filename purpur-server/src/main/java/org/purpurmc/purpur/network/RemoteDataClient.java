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
 * RemoteDataClient – SLAVE server side (v5, block + chat sync).
 *
 * Added in v5:
 *   sendChatEventDirect(ChatSyncMessage) – blocking CHAT_ACTION to master (call from non-main thread).
 *   Incoming CHAT_PUSH (requestId=0) decoded and applied via RemoteChatListener.applyPush().
 */
public class RemoteDataClient {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataClient");

    // ── Singleton ─────────────────────────────────────────────────────────────
    private static RemoteDataClient INSTANCE;
    public static RemoteDataClient get()                      { return INSTANCE; }
    public static RemoteDataClient init(RemoteDataConfig cfg) { INSTANCE = new RemoteDataClient(cfg); return INSTANCE; }

    // ── Fields ────────────────────────────────────────────────────────────────
    private final RemoteDataConfig config;
    private final HmacHelper       hmac;
    private final AtomicInteger    requestIdGen = new AtomicInteger(1);

    private final ConcurrentHashMap<Integer, CompletableFuture<RemoteDataPacket>> pending      = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String,  CompletableFuture<Boolean>>          inFlightPuts = new ConcurrentHashMap<>();

    private volatile Socket           socket;
    private volatile DataInputStream  dis;
    private volatile DataOutputStream dos;
    private volatile boolean          connected  = false;
    private volatile boolean          running    = false;

    private Thread readerThread;
    private Thread reconnectThread;

    // ── Constructor ───────────────────────────────────────────────────────────
    public RemoteDataClient(RemoteDataConfig config) {
        this.config = config;
        this.hmac   = new HmacHelper(config.secretKey);
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
        if (readerThread    != null) readerThread.interrupt();
        closeSocket();
        LOGGER.info("[RemoteData] Client stopped.");
    }

    public boolean isConnected() { return connected; }

    // ── Public API ────────────────────────────────────────────────────────────

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
        CompletableFuture<Boolean> mine     = new CompletableFuture<>();
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
     * Send a BLOCK_ACTION to master.
     * Fire-and-forget on a virtual thread – never blocks the Minecraft main thread.
     */
    public void sendBlockAction(BlockSyncMessage msg) {
        if (!connected) {
            LOGGER.fine("[RemoteData] sendBlockAction skipped – not connected");
            return;
        }
        Thread.ofVirtual().name("RemoteData-BlockAction").start(() -> {
            try {
                byte[] encoded = msg.encode();
                sendAndWait(
                        new RemoteDataPacket(RemoteDataPacket.OpCode.BLOCK_ACTION, nextId(), "", encoded),
                        config.shortTimeoutMs);
            } catch (Exception e) {
                LOGGER.log(Level.FINE, "[RemoteData] BLOCK_ACTION failed pos=" + msg.posKey(), e);
            }
        });
    }

    /**
     * Send a CHAT_ACTION to master (chat, advancement, join, quit, death, command).
     * Called directly – must already be on a non-main thread (virtual thread is fine).
     */
    public void sendChatEventDirect(ChatSyncMessage msg) {
        if (!connected) {
            LOGGER.fine("[RemoteData] sendChatEventDirect skipped – not connected");
            return;
        }
        try {
            byte[] encoded = msg.encode();
            sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.CHAT_ACTION, nextId(), "", encoded),
                    config.shortTimeoutMs);
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "[RemoteData] CHAT_ACTION failed: " + msg, e);
        }
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
            long t    = System.currentTimeMillis();
            RemoteDataPacket resp = sendAndWait(
                    new RemoteDataPacket(RemoteDataPacket.OpCode.PING, nextId(), "", null),
                    config.shortTimeoutMs);
            return resp.opCode == RemoteDataPacket.OpCode.PONG ? System.currentTimeMillis() - t : -1;
        } catch (Exception e) { return -1; }
    }

    // ── Internal ──────────────────────────────────────────────────────────────

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
            throw new IOException("Timed out after " + timeoutMs + "ms: " + pkt.opCode);
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
                    if (pkt.requestId == 0) {
                        handleServerPush(pkt);
                        continue;
                    }
                    CompletableFuture<RemoteDataPacket> future = pending.remove(pkt.requestId);
                    if (future != null) future.complete(pkt);
                    else LOGGER.fine("[RemoteData] No pending future for requestId=" + pkt.requestId);
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
     * Handle unsolicited packets pushed from master (requestId=0).
     *
     * BLOCK_PUSH – decode and apply block change on main thread.
     * CHAT_PUSH  – decode and broadcast to all players on this server.
     * CHUNK_PUSH – update local cache.
     */
    private void handleServerPush(RemoteDataPacket pkt) {
        switch (pkt.opCode) {

            case BLOCK_PUSH -> {
                try {
                    BlockSyncMessage msg = BlockSyncMessage.decode(pkt.data);
                    RemoteBlockHandler.applyPush(msg);
                    LOGGER.fine("[RemoteData] Received BLOCK_PUSH: " + msg);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "[RemoteData] BLOCK_PUSH decode/apply failed", e);
                }
            }

            case CHAT_PUSH -> {
                try {
                    ChatSyncMessage msg = ChatSyncMessage.decode(pkt.data);
                    // applyPush is async-safe (uses Bukkit.broadcast which handles threading)
                    RemoteChatListener.applyPush(msg);
                    LOGGER.fine("[RemoteData] Received CHAT_PUSH: " + msg);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "[RemoteData] CHAT_PUSH decode/apply failed", e);
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
            }

            default -> LOGGER.fine("[RemoteData] Unknown server push opCode=" + pkt.opCode);
        }
    }

    // ── Reconnect loop ────────────────────────────────────────────────────────

    private void reconnectLoop() {
        long backoffMs = 1_000;
        while (running) {
            if (!connected) {
                try {
                    connectToMaster();
                    backoffMs = 1_000;
                } catch (Exception e) {
                    LOGGER.warning("[RemoteData] Could not connect (" + e.getMessage()
                            + "). Retrying in " + (backoffMs / 1000) + "s...");
                    try { Thread.sleep(backoffMs); }
                    catch (InterruptedException ie) { break; }
                    backoffMs = Math.min(backoffMs * 2, 30_000);
                }
            } else {
                try { Thread.sleep(2_000); }
                catch (InterruptedException e) { break; }
            }
        }
    }

    private void connectToMaster() throws IOException {
        closeSocket();
        Socket s = config.useTLS ? buildTLSSocket() : new Socket();
        if (!config.useTLS) {
            s.connect(new java.net.InetSocketAddress(config.masterHost, config.masterPort), config.connectTimeoutMs);
        }
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
                connected = false;
                closeSocket();
                throw new IOException("Authentication rejected by master");
            }
        } catch (TimeoutException e) {
            connected = false; closeSocket(); throw new IOException("Auth timeout");
        } catch (ExecutionException | InterruptedException e) {
            connected = false; closeSocket(); throw new IOException("Auth failed: " + e.getMessage());
        }

        LOGGER.info("[RemoteData] Connected and authenticated to master "
                + config.masterHost + ":" + config.masterPort);
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
        if (s != null && !s.isClosed()) {
            try { s.close(); } catch (IOException ignored) {}
        }
        socket    = null;
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
            SSLSocket ss = (SSLSocket) ctx.getSocketFactory().createSocket();
            ss.connect(new java.net.InetSocketAddress(config.masterHost, config.masterPort), config.connectTimeoutMs);
            ss.startHandshake();
            return ss;
        } catch (Exception e) {
            throw new IOException("TLS setup failed: " + e.getMessage(), e);
        }
    }
}
