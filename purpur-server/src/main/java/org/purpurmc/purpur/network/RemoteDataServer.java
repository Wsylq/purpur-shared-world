package org.purpurmc.purpur.network;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.security.KeyStore;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataServer – MASTER server (v5, block + chat sync).
 *
 * Added in v5:
 *   broadcastChatPublic(ChatSyncMessage) – broadcast CHAT_PUSH to all slaves.
 *   CHAT_ACTION handler – receive chat event from slave, apply locally on master,
 *   then broadcast CHAT_PUSH to ALL slaves (including the sender, so its players see it).
 */
public class RemoteDataServer {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataServer");

    // ── Singleton ─────────────────────────────────────────────────────────────
    private static RemoteDataServer INSTANCE;
    public static RemoteDataServer get()                                          { return INSTANCE; }
    public static RemoteDataServer init(RemoteDataConfig config, File serverRoot) {
        INSTANCE = new RemoteDataServer(config, serverRoot);
        return INSTANCE;
    }

    // ── Fields ────────────────────────────────────────────────────────────────
    private final RemoteDataConfig  config;
    private final HmacHelper        hmac;
    private final RemoteDataStorage storage;

    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private Thread acceptThread;

    /** Connected + authenticated slave handlers keyed by remote address string. */
    private final ConcurrentHashMap<String, ClientHandler> clients = new ConcurrentHashMap<>();

    /** Conflict guard: "world/x/y/z" → last accepted timestamp (ms) */
    private final ConcurrentHashMap<String, Long> lastBlockTimestamp = new ConcurrentHashMap<>();

    private final ExecutorService clientPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "RemoteData-ClientHandler");
        t.setDaemon(true);
        return t;
    });

    // ── Constructor ───────────────────────────────────────────────────────────
    private RemoteDataServer(RemoteDataConfig config, File serverRoot) {
        this.config  = config;
        this.hmac    = new HmacHelper(config.secretKey);
        this.storage = new RemoteDataStorage(new File(serverRoot, "remote-storage"));
    }

    public RemoteDataStorage getStorage() { return storage; }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    public void start() throws IOException {
        if (config.useTLS) {
            serverSocket = buildTLSServerSocket();
        } else {
            ServerSocket ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(config.masterPort));
            serverSocket = ss;
        }
        running      = true;
        acceptThread = new Thread(this::acceptLoop, "RemoteData-Accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        LOGGER.info("[RemoteData] Master server listening on port " + config.masterPort
                + (config.useTLS ? " [TLS]" : " [plain TCP]"));
    }

    public void stop() {
        running = false;
        try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) {}
        clients.values().forEach(ClientHandler::close);
        clients.clear();
        clientPool.shutdownNow();
        LOGGER.info("[RemoteData] Master server stopped.");
    }

    // ── Accept loop ───────────────────────────────────────────────────────────

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = serverSocket.accept();
                s.setTcpNoDelay(true);
                s.setKeepAlive(true);
                s.setSendBufferSize(256 * 1024);
                s.setReceiveBufferSize(256 * 1024);
                clientPool.submit(new ClientHandler(s));
            } catch (IOException e) {
                if (running) LOGGER.log(Level.WARNING, "[RemoteData] Accept error", e);
            }
        }
    }

    // ── Public broadcast API ──────────────────────────────────────────────────

    /**
     * Broadcast BLOCK_PUSH to ALL authenticated slaves.
     * Called from RemoteBlockListener (MONITOR) when a block changes on master directly.
     */
    public void broadcastBlockPushPublic(BlockSyncMessage msg) {
        broadcastBlockPush(msg);
    }

    /**
     * Broadcast CHAT_PUSH to ALL authenticated slaves.
     * Called from RemoteChatListener (MONITOR) when a chat event fires on master,
     * and from CHAT_ACTION handler when a slave relays an event.
     */
    public void broadcastChatPublic(ChatSyncMessage msg) {
        broadcastChatPush(msg);
    }

    // ── Private broadcast helpers ─────────────────────────────────────────────

    private void broadcastBlockPush(BlockSyncMessage msg) {
        byte[] encoded;
        try { encoded = msg.encode(); }
        catch (IOException e) {
            LOGGER.warning("[RemoteData] broadcastBlockPush encode failed: " + e.getMessage());
            return;
        }
        RemoteDataPacket pkt = new RemoteDataPacket(RemoteDataPacket.OpCode.BLOCK_PUSH, 0, "", encoded);
        int sent = 0;
        for (ClientHandler target : clients.values()) {
            if (!target.authenticated) continue;
            Thread.ofVirtual().name("RemoteData-BlockPush").start(() -> {
                try { target.sendPacket(pkt); }
                catch (IOException e) { LOGGER.fine("[RemoteData] BLOCK_PUSH send failed: " + e.getMessage()); }
            });
            sent++;
        }
        LOGGER.fine("[RemoteData] Broadcast BLOCK_PUSH " + msg + " to " + sent + " slave(s)");
    }

    private void broadcastChatPush(ChatSyncMessage msg) {
        byte[] encoded;
        try { encoded = msg.encode(); }
        catch (IOException e) {
            LOGGER.warning("[RemoteData] broadcastChatPush encode failed: " + e.getMessage());
            return;
        }
        RemoteDataPacket pkt = new RemoteDataPacket(RemoteDataPacket.OpCode.CHAT_PUSH, 0, "", encoded);
        int sent = 0;
        for (ClientHandler target : clients.values()) {
            if (!target.authenticated) continue;
            Thread.ofVirtual().name("RemoteData-ChatPush").start(() -> {
                try { target.sendPacket(pkt); }
                catch (IOException e) { LOGGER.fine("[RemoteData] CHAT_PUSH send failed: " + e.getMessage()); }
            });
            sent++;
        }
        LOGGER.fine("[RemoteData] Broadcast CHAT_PUSH " + msg + " to " + sent + " slave(s)");
    }

    private void broadcastChunkPush(String senderClientId, String chunkKey, byte[] chunkData) {
        RemoteDataPacket pkt = new RemoteDataPacket(RemoteDataPacket.OpCode.CHUNK_PUSH, 0, chunkKey, chunkData);
        for (var entry : clients.entrySet()) {
            if (entry.getKey().equals(senderClientId)) continue;
            ClientHandler target = entry.getValue();
            if (!target.authenticated) continue;
            Thread.ofVirtual().name("RemoteData-ChunkPush-" + entry.getKey()).start(() -> {
                try { target.sendPacket(pkt); }
                catch (IOException e) {
                    LOGGER.warning("[RemoteData] CHUNK_PUSH to " + entry.getKey() + " failed: " + e.getMessage());
                }
            });
        }
    }

    // ── Client handler ────────────────────────────────────────────────────────

    private class ClientHandler implements Runnable {
        private final Socket socket;
        private DataInputStream  dis;
        private DataOutputStream dos;
        volatile boolean authenticated = false;
        private final String id;

        ClientHandler(Socket socket) {
            this.socket = socket;
            this.id     = socket.getRemoteSocketAddress().toString();
        }

        @Override
        public void run() {
            clients.put(id, this);
            try {
                dis = new DataInputStream (new BufferedInputStream (socket.getInputStream(),  65536));
                dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65536));
                LOGGER.info("[RemoteData] Slave connected: " + id);
                while (running && !socket.isClosed()) handlePacket();
            } catch (EOFException | SocketException e) {
                // normal disconnect
            } catch (IOException e) {
                if (running) LOGGER.log(Level.WARNING, "[RemoteData] Error with slave " + id, e);
            } finally {
                clients.remove(id);
                close();
                LOGGER.info("[RemoteData] Slave disconnected: " + id);
            }
        }

        private void handlePacket() throws IOException {
            RemoteDataPacket pkt = RemoteDataPacket.readFrom(dis, hmac);

            // ── Auth gate ─────────────────────────────────────────────────
            if (!authenticated) {
                if (pkt.opCode == RemoteDataPacket.OpCode.AUTH) {
                    String supplied = new String(pkt.data, java.nio.charset.StandardCharsets.UTF_8);
                    if (config.secretKey.equals(supplied)) {
                        authenticated = true;
                        sendResponse(RemoteDataPacket.OpCode.AUTH_OK, pkt.requestId, "auth", "OK".getBytes());
                        LOGGER.info("[RemoteData] Slave authenticated: " + id);
                    } else {
                        sendResponse(RemoteDataPacket.OpCode.AUTH_FAIL, pkt.requestId, "auth", "FAIL".getBytes());
                        close();
                    }
                } else {
                    sendResponse(RemoteDataPacket.OpCode.AUTH_FAIL, pkt.requestId, "", "Not authenticated".getBytes());
                    close();
                }
                return;
            }

            // ── Dispatch ──────────────────────────────────────────────────
            switch (pkt.opCode) {

                case PING -> sendResponse(RemoteDataPacket.OpCode.PONG, pkt.requestId, "", new byte[0]);

                // ── Block action: slave → master → all slaves ─────────────
                case BLOCK_ACTION -> {
                    sendResponse(RemoteDataPacket.OpCode.OK, pkt.requestId, "", new byte[0]);
                    try {
                        BlockSyncMessage msg = BlockSyncMessage.decode(pkt.data);
                        String posKey = msg.posKey();

                        long prev = lastBlockTimestamp.getOrDefault(posKey, Long.MIN_VALUE);
                        if (msg.timestamp <= prev) {
                            LOGGER.fine("[RemoteData] BLOCK_ACTION stale, ignoring: " + msg);
                            return;
                        }
                        lastBlockTimestamp.put(posKey, msg.timestamp);

                        // Apply to master world
                        RemoteBlockHandler.applyPush(msg);
                        // Broadcast resulting state to ALL slaves
                        broadcastBlockPush(msg);

                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "[RemoteData] BLOCK_ACTION processing error", e);
                    }
                }

                // ── Chat action: slave → master → all slaves ──────────────
                case CHAT_ACTION -> {
                    sendResponse(RemoteDataPacket.OpCode.OK, pkt.requestId, "", new byte[0]);
                    try {
                        ChatSyncMessage msg = ChatSyncMessage.decode(pkt.data);

                        // Apply (broadcast) on master server so master players see it
                        RemoteChatListener.applyPush(msg);
                        // Forward to ALL slaves (including the originating slave,
                        // so its players see the message from the master echo)
                        broadcastChatPush(msg);

                        LOGGER.fine("[RemoteData] CHAT_ACTION processed: " + msg);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "[RemoteData] CHAT_ACTION processing error", e);
                    }
                }

                // ── Legacy storage ops ────────────────────────────────────
                case GET -> {
                    byte[] data = storage.get(pkt.key);
                    if (data != null) sendResponse(RemoteDataPacket.OpCode.DATA,      pkt.requestId, pkt.key, data);
                    else              sendResponse(RemoteDataPacket.OpCode.NOT_FOUND,  pkt.requestId, pkt.key, new byte[0]);
                }
                case PUT -> {
                    storage.put(pkt.key, pkt.data);
                    sendResponse(RemoteDataPacket.OpCode.OK, pkt.requestId, pkt.key, new byte[0]);
                    if (pkt.key.startsWith("chunk/")) broadcastChunkPush(id, pkt.key, pkt.data);
                }
                case DELETE -> {
                    storage.delete(pkt.key);
                    sendResponse(RemoteDataPacket.OpCode.OK, pkt.requestId, pkt.key, new byte[0]);
                }
                case LIST -> {
                    String[] keys  = storage.list(pkt.key);
                    byte[]   result = String.join("\n", keys)
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    sendResponse(RemoteDataPacket.OpCode.LIST_RESULT, pkt.requestId, pkt.key, result);
                }
                case BATCH_PUT -> {
                    parseBatchPut(pkt);
                    sendResponse(RemoteDataPacket.OpCode.OK, pkt.requestId, "", new byte[0]);
                }
                case CHUNK_INVALIDATE -> {
                    byte[] data = storage.get(pkt.key);
                    if (data != null) broadcastChunkPush(id, pkt.key, data);
                    sendResponse(RemoteDataPacket.OpCode.OK, pkt.requestId, pkt.key, new byte[0]);
                }

                default -> sendResponse(RemoteDataPacket.OpCode.ERROR, pkt.requestId, "",
                        ("Unknown op: " + pkt.opCode).getBytes());
            }
        }

        private void parseBatchPut(RemoteDataPacket pkt) throws IOException {
            DataInputStream batch = new DataInputStream(new ByteArrayInputStream(pkt.data));
            int count = batch.readShort() & 0xFFFF;
            for (int i = 0; i < count; i++) {
                int    kLen = batch.readShort() & 0xFFFF;
                byte[] kb   = new byte[kLen];
                batch.readFully(kb);
                String key  = new String(kb, java.nio.charset.StandardCharsets.UTF_8);
                int    dLen = batch.readInt();
                byte[] data = new byte[dLen];
                batch.readFully(data);
                storage.put(key, data);
                if (key.startsWith("chunk/")) broadcastChunkPush(id, key, data);
            }
        }

        void sendPacket(RemoteDataPacket pkt) throws IOException {
            synchronized (dos) {
                pkt.writeTo(dos, hmac, config.compressionEnabled);
                dos.flush();
            }
        }

        private void sendResponse(RemoteDataPacket.OpCode op, int reqId, String key, byte[] data) throws IOException {
            sendPacket(new RemoteDataPacket(op, reqId, key, data));
        }

        void close() {
            try { if (!socket.isClosed()) socket.close(); } catch (IOException ignored) {}
        }
    }

    // ── TLS ───────────────────────────────────────────────────────────────────

    private ServerSocket buildTLSServerSocket() throws IOException {
        try {
            KeyStore ks = KeyStore.getInstance("JKS");
            try (FileInputStream fis = new FileInputStream(config.tlsKeystorePath)) {
                ks.load(fis, config.tlsKeystorePassword.toCharArray());
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, config.tlsKeystorePassword.toCharArray());
            SSLContext ctx = SSLContext.getInstance("TLSv1.3");
            ctx.init(kmf.getKeyManagers(), null, null);
            SSLServerSocket ss = (SSLServerSocket) ctx.getServerSocketFactory().createServerSocket(config.masterPort);
            ss.setReuseAddress(true);
            return ss;
        } catch (Exception e) {
            throw new IOException("TLS server socket setup failed: " + e.getMessage(), e);
        }
    }
}
