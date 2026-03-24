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
 * RemoteDataServer
 *
 * Runs on the MASTER server only.
 * Accepts TCP connections from player servers, authenticates them, then
 * processes GET / PUT / DELETE / LIST / BATCH_PUT / PING / CHUNK_INVALIDATE operations.
 *
 * Real-time chunk sync:
 *   When a client PUTs a chunk key, the master stores it AND broadcasts
 *   a CHUNK_PUSH packet (containing the full chunk data) to ALL other
 *   authenticated clients immediately. This ensures every player server
 *   gets the updated chunk data in real-time without polling.
 *
 * Storage backend: RemoteDataStorage (pluggable – default is flat-file).
 *
 * One thread per connected client (thread-per-connection model).
 * Suitable for a small number of player servers (<= 20).
 */
public class RemoteDataServer {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataServer");

    // ── Singleton ─────────────────────────────────────────────────────────────
    private static RemoteDataServer INSTANCE;

    public static RemoteDataServer get() { return INSTANCE; }

    public static RemoteDataServer init(RemoteDataConfig config, File serverRoot) {
        INSTANCE = new RemoteDataServer(config, serverRoot);
        return INSTANCE;
    }

    // ── Fields ────────────────────────────────────────────────────────────────
    private final RemoteDataConfig config;
    private final HmacHelper hmac;
    private final RemoteDataStorage storage;
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private Thread acceptThread;

    /** All authenticated connected clients: id → handler */
    private final ConcurrentHashMap<String, ClientHandler> clients = new ConcurrentHashMap<>();

    /** Thread pool for client handlers */
    private final ExecutorService clientPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "RemoteData-ClientHandler");
        t.setDaemon(true);
        return t;
    });

    // ── Constructor ───────────────────────────────────────────────────────────
    private RemoteDataServer(RemoteDataConfig config, File serverRoot) {
        this.config = config;
        this.hmac = new HmacHelper(config.secretKey);
        this.storage = new RemoteDataStorage(new File(serverRoot, "remote-storage"));
    }

    public RemoteDataStorage getStorage() { return storage; }

    // ── Lifecycle ─────────────────────────────────────────────────────────────
    public void start() throws IOException {
        if (config.useTLS) {
            serverSocket = buildTLSServerSocket();
        } else {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(config.masterPort));
        }
        running = true;
        acceptThread = new Thread(this::acceptLoop, "RemoteData-Accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
        LOGGER.info("[RemoteData] Master server listening on port " + config.masterPort
                + (config.useTLS ? " [TLS]" : " [plain TCP]"));
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
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
                ClientHandler handler = new ClientHandler(s);
                clientPool.submit(handler);
            } catch (IOException e) {
                if (running) {
                    LOGGER.log(Level.WARNING, "[RemoteData] Accept error", e);
                }
            }
        }
    }

    // ── Broadcast helpers ─────────────────────────────────────────────────────

    /**
     * Broadcast a CHUNK_PUSH packet to all authenticated clients EXCEPT the sender.
     * This is the core of real-time shared world sync.
     *
     * @param senderClientId  the client ID of the server that sent the chunk update (excluded from broadcast)
     * @param chunkKey        the chunk key e.g. "chunk/world/4/-9"
     * @param chunkData       the full chunk NBT bytes
     */
    private void broadcastChunkPush(String senderClientId, String chunkKey, byte[] chunkData) {
        RemoteDataPacket pushPkt = new RemoteDataPacket(
                RemoteDataPacket.OpCode.CHUNK_PUSH,
                0, // requestId 0 = unsolicited push, no response expected
                chunkKey,
                chunkData
        );

        int sent = 0;
        for (java.util.Map.Entry<String, ClientHandler> entry : clients.entrySet()) {
            // Skip the sender
            if (entry.getKey().equals(senderClientId)) continue;
            ClientHandler target = entry.getValue();
            if (!target.authenticated) continue;
            try {
                target.sendPacket(pushPkt);
                sent++;
            } catch (IOException e) {
                LOGGER.warning("[RemoteData] Failed to push chunk " + chunkKey
                        + " to client " + entry.getKey() + ": " + e.getMessage());
            }
        }

        if (sent > 0) {
            LOGGER.fine("[RemoteData] Broadcast CHUNK_PUSH key=" + chunkKey + " to " + sent + " client(s)");
        }
    }

    // ── Client handler ────────────────────────────────────────────────────────
    private class ClientHandler implements Runnable {
        private final Socket socket;
        private DataInputStream dis;
        private DataOutputStream dos;
        volatile boolean authenticated = false;
        private final String id;

        ClientHandler(Socket socket) {
            this.socket = socket;
            this.id = socket.getRemoteSocketAddress().toString();
        }

        @Override
        public void run() {
            clients.put(id, this);
            try {
                dis = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 65536));
                dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65536));
                LOGGER.info("[RemoteData] Client connected: " + id);

                while (running && !socket.isClosed()) {
                    handlePacket();
                }
            } catch (EOFException | SocketException e) {
                // Client disconnected normally
            } catch (IOException e) {
                if (running) {
                    LOGGER.log(Level.WARNING, "[RemoteData] Error with client " + id + ": " + e.getMessage());
                }
            } finally {
                clients.remove(id);
                close();
                LOGGER.info("[RemoteData] Client disconnected: " + id);
            }
        }

        private void handlePacket() throws IOException {
            RemoteDataPacket pkt = RemoteDataPacket.readFrom(dis, hmac);

            // Must authenticate first
            if (!authenticated) {
                if (pkt.opCode == RemoteDataPacket.OpCode.AUTH) {
                    String suppliedKey = new String(pkt.data, java.nio.charset.StandardCharsets.UTF_8);
                    if (config.secretKey.equals(suppliedKey)) {
                        authenticated = true;
                        sendResponse(RemoteDataPacket.OpCode.AUTH_OK, pkt.requestId, "auth", "OK".getBytes());
                        LOGGER.info("[RemoteData] Client authenticated: " + id);
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

            switch (pkt.opCode) {
                case PING -> sendResponse(RemoteDataPacket.OpCode.PONG, pkt.requestId, "", new byte[0]);

                case GET -> {
                    byte[] data = storage.get(pkt.key);
                    if (data != null) {
                        sendResponse(RemoteDataPacket.OpCode.DATA, pkt.requestId, pkt.key, data);
                    } else {
                        sendResponse(RemoteDataPacket.OpCode.NOT_FOUND, pkt.requestId, pkt.key, new byte[0]);
                    }
                }

                case PUT -> {
                    storage.put(pkt.key, pkt.data);
                    sendResponse(RemoteDataPacket.OpCode.OK, pkt.requestId, pkt.key, new byte[0]);
                    LOGGER.info("[RemoteData] PUT " + pkt.key);
                    // ── Real-time sync: if this is a chunk key, push to all other clients immediately ──
                    if (pkt.key.startsWith("chunk/")) {
                        broadcastChunkPush(id, pkt.key, pkt.data);
                    }
                }

                case DELETE -> {
                    storage.delete(pkt.key);
                    sendResponse(RemoteDataPacket.OpCode.OK, pkt.requestId, pkt.key, new byte[0]);
                }

                case LIST -> {
                    String[] keys = storage.list(pkt.key);
                    byte[] result = String.join("\n", keys).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    sendResponse(RemoteDataPacket.OpCode.LIST_RESULT, pkt.requestId, pkt.key, result);
                }

                case BATCH_PUT -> {
                    parseBatchPut(pkt);
                    sendResponse(RemoteDataPacket.OpCode.OK, pkt.requestId, "", new byte[0]);
                }

                case CHUNK_INVALIDATE -> {
                    // A client explicitly requests invalidation broadcast (no data, just key)
                    // Used when a client wants others to re-fetch a chunk without sending the full data
                    // We fetch from storage and push to all others
                    byte[] data = storage.get(pkt.key);
                    if (data != null) {
                        broadcastChunkPush(id, pkt.key, data);
                    }
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
                int keyLen = batch.readShort() & 0xFFFF;
                byte[] keyBytes = new byte[keyLen];
                batch.readFully(keyBytes);
                String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                int dataLen = batch.readInt();
                byte[] data = new byte[dataLen];
                batch.readFully(data);
                storage.put(key, data);

                // Broadcast chunk pushes from batch as well
                if (key.startsWith("chunk/")) {
                    broadcastChunkPush(id, key, data);
                }
            }
        }

        void sendPacket(RemoteDataPacket pkt) throws IOException {
            synchronized (dos) {
                pkt.writeTo(dos, hmac, config.compressionEnabled);
            }
        }

        private void sendResponse(RemoteDataPacket.OpCode op, int requestId, String key, byte[] data) throws IOException {
            sendPacket(new RemoteDataPacket(op, requestId, key, data));
        }

        void close() {
            try {
                if (!socket.isClosed()) socket.close();
            } catch (IOException ignored) {}
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
