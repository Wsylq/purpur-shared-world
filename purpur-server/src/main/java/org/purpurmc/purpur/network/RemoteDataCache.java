package org.purpurmc.purpur.network;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataCache
 *
 * Two-tier local cache sitting between the Minecraft server and the remote master.
 *
 * Tier 1 – RAM (ConcurrentHashMap with LRU eviction)
 * Tier 2 – Local disk (flat key→file mapping under ./remote-cache/)
 *
 * Dirty tracking: any PUT marks the entry dirty. A background thread
 * periodically flushes dirty entries to the master via RemoteDataClient.
 * NOTE: With FIX #2 applied, RemoteChunkDataHandler.save() now PUTs to the
 * master immediately and asynchronously, so the flush thread is a safety-net
 * for disconnected/retry scenarios only.
 *
 * Write-Ahead Log (WAL): every write is appended to a WAL file before
 * being acknowledged. On startup the WAL is replayed so no writes are
 * lost even if the server crashes before a flush.
 *
 * Key naming convention (imposed by callers):
 *   "player/<uuid>"             → player NBT blob
 *   "chunk/<world>/<x>/<z>"    → chunk NBT blob
 *   "cfg/<name>"               → config file bytes
 *   "plugin/<name>/<key>"      → plugin data file bytes
 */
public class RemoteDataCache {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataCache");

    // ── Singleton ─────────────────────────────────────────────────────────────
    private static RemoteDataCache INSTANCE;

    public static RemoteDataCache get() {
        return INSTANCE;
    }

    public static RemoteDataCache init(RemoteDataConfig config, File serverRoot) {
        INSTANCE = new RemoteDataCache(config, serverRoot);
        return INSTANCE;
    }

    // ── LRU RAM cache entry ───────────────────────────────────────────────────
    private static class Entry {
        volatile byte[] data;
        volatile boolean dirty;
        volatile long lastAccessNs;

        Entry(byte[] data, boolean dirty) {
            this.data = data;
            this.dirty = dirty;
            this.lastAccessNs = System.nanoTime();
        }

        void touch() {
            lastAccessNs = System.nanoTime();
        }
    }

    // ── Fields ────────────────────────────────────────────────────────────────
    private final RemoteDataConfig config;
    private final File cacheDir;
    private final File walFile;

    /** RAM cache */
    private final ConcurrentHashMap<String, Entry> ramCache = new ConcurrentHashMap<>();

    /** Dirty keys waiting to be flushed to master (fallback / retry path) */
    private final ConcurrentLinkedQueue<String> dirtyQueue = new ConcurrentLinkedQueue<>();

    /** WAL stream (append-only) */
    private DataOutputStream walOut;
    private final Object walLock = new Object();

    /** Stats */
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();

    private volatile boolean running = false;
    private Thread flushThread;

    // ── Constructor ───────────────────────────────────────────────────────────
    private RemoteDataCache(RemoteDataConfig config, File serverRoot) {
        this.config = config;
        this.cacheDir = new File(serverRoot, "remote-cache");
        this.walFile = new File(serverRoot, config.walPath);
        cacheDir.mkdirs();
        if (config.walEnabled) {
            openWAL();
            replayWAL();
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Get data for key.
     * Order: RAM → disk → null (caller fetches from master directly).
     */
    public byte[] get(String key) {
        Entry e = ramCache.get(key);
        if (e != null) {
            e.touch();
            hits.incrementAndGet();
            return e.data;
        }

        byte[] diskData = readDisk(key);
        if (diskData != null) {
            putRam(key, diskData, false);
            hits.incrementAndGet();
            return diskData;
        }

        misses.incrementAndGet();
        return null;
    }

    /**
     * Put data into cache (RAM + disk) and mark dirty.
     * The background flush thread will push to master as a retry/fallback.
     * (The primary push is now done immediately in RemoteChunkDataHandler.save.)
     */
    public void put(String key, byte[] data) {
        if (config.walEnabled) {
            appendWAL(key, data);
        }
        putRam(key, data, true);
        writeDisk(key, data);
        dirtyQueue.offer(key);
        evictIfNeeded();
    }

    /**
     * Write data directly to RAM+disk without marking dirty.
     * Used when data just arrived from the master (no need to send it back).
     */
    public void putClean(String key, byte[] data) {
        putRam(key, data, false);
        writeDisk(key, data);
    }

    /**
     * Remove from cache (RAM + disk). Does NOT delete on master.
     */
    public void invalidate(String key) {
        ramCache.remove(key);
        deleteDisk(key);
    }

    /**
     * Returns all dirty keys in the RAM cache (for batch sync fallback).
     */
    public Set<String> dirtyKeys() {
        Set<String> keys = new HashSet<>();
        ramCache.forEach((k, e) -> {
            if (e.dirty) keys.add(k);
        });
        return keys;
    }

    /**
     * Mark a key as no longer dirty (after successful push to master).
     */
    public void markClean(String key) {
        Entry e = ramCache.get(key);
        if (e != null) e.dirty = false;
    }

    /**
     * Peek at cached data without marking dirty (for read-through).
     */
    public byte[] peek(String key) {
        Entry e = ramCache.get(key);
        return e != null ? e.data : null;
    }

    // ── Background flush thread ───────────────────────────────────────────────

    public void start() {
        running = true;
        flushThread = new Thread(this::flushLoop, "RemoteData-Flush");
        flushThread.setDaemon(true);
        flushThread.start();
        LOGGER.info("[RemoteData] Cache started. WAL=" + config.walEnabled
                + " maxEntries=" + config.maxChunkCacheEntries);
    }

    public void stop() {
        running = false;
        if (flushThread != null) {
            flushThread.interrupt();
            try { flushThread.join(5000); } catch (InterruptedException ignored) {}
        }
        flushDirty(true);
        closeWAL();
        LOGGER.info("[RemoteData] Cache stopped. hits=" + hits.get() + " misses=" + misses.get());
    }

    private void flushLoop() {
        long intervalMs = (long) config.dirtyFlushIntervalTicks * 50L; // ticks → ms
        while (running) {
            try {
                Thread.sleep(intervalMs);
                flushDirty(false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Flush dirty entries to master via RemoteDataClient.
     * This is the retry/fallback path; primary pushes happen immediately in save().
     *
     * @param forceAll flush ALL dirty entries (used on shutdown)
     */
    private void flushDirty(boolean forceAll) {
        RemoteDataClient client = RemoteDataClient.get();
        if (client == null || !client.isConnected()) return;

        List<String> batch = new ArrayList<>(config.batchSize);
        List<byte[]> batchData = new ArrayList<>(config.batchSize);

        Set<String> dirty = dirtyKeys();
        for (String key : dirty) {
            Entry e = ramCache.get(key);
            if (e == null || !e.dirty) continue;
            batch.add(key);
            batchData.add(e.data);
            if (!forceAll && batch.size() >= config.batchSize) {
                sendBatch(client, batch, batchData);
                batch.clear();
                batchData.clear();
            }
        }

        if (!batch.isEmpty()) {
            sendBatch(client, batch, batchData);
        }
    }

    private void sendBatch(RemoteDataClient client, List<String> keys, List<byte[]> dataList) {
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            byte[] data = dataList.get(i);
            if (client.put(key, data)) {
                markClean(key);
            }
        }
    }

    // ── RAM cache helpers ─────────────────────────────────────────────────────

    private void putRam(String key, byte[] data, boolean dirty) {
        Entry existing = ramCache.get(key);
        if (existing != null) {
            existing.data = data;
            existing.dirty = dirty;
            existing.touch();
        } else {
            ramCache.put(key, new Entry(data, dirty));
        }
    }

    private void evictIfNeeded() {
        if (ramCache.size() <= config.maxChunkCacheEntries) return;

        String lruKey = null;
        long oldestNs = Long.MAX_VALUE;
        for (Map.Entry<String, Entry> me : ramCache.entrySet()) {
            if (!me.getValue().dirty && me.getValue().lastAccessNs < oldestNs) {
                oldestNs = me.getValue().lastAccessNs;
                lruKey = me.getKey();
            }
        }
        if (lruKey != null) ramCache.remove(lruKey);
    }

    // ── Disk helpers ──────────────────────────────────────────────────────────

    private File diskFile(String key) {
        String safePath = key.replace('/', File.separatorChar);
        File candidate = new File(cacheDir, safePath);
        if (!candidate.getAbsolutePath().startsWith(cacheDir.getAbsolutePath())) {
            throw new SecurityException("Path traversal attempt in key: " + key);
        }
        return new File(candidate.getAbsolutePath() + ".dat");
    }

    private byte[] readDisk(String key) {
        File f = diskFile(key);
        if (!f.exists()) return null;
        try {
            return Files.readAllBytes(f.toPath());
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Disk read failed: " + key, e);
            return null;
        }
    }

    private void writeDisk(String key, byte[] data) {
        File f = diskFile(key);
        f.getParentFile().mkdirs();
        try {
            Files.write(f.toPath(), data, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Disk write failed: " + key, e);
        }
    }

    private void deleteDisk(String key) {
        File f = diskFile(key);
        if (f.exists()) f.delete();
    }

    // ── WAL ───────────────────────────────────────────────────────────────────

    private static final int WAL_MAGIC = 0xCAFEBABE;

    private void openWAL() {
        synchronized (walLock) {
            try {
                walOut = new DataOutputStream(new BufferedOutputStream(
                        new FileOutputStream(walFile, true), 65536));
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "[RemoteData] Could not open WAL file", e);
            }
        }
    }

    private void appendWAL(String key, byte[] data) {
        synchronized (walLock) {
            if (walOut == null) return;
            try {
                byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                walOut.writeInt(WAL_MAGIC);
                walOut.writeShort(keyBytes.length);
                walOut.write(keyBytes);
                walOut.writeInt(data.length);
                walOut.write(data);
                walOut.flush();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "[RemoteData] WAL append failed", e);
            }
        }
    }

    private void replayWAL() {
        if (!walFile.exists() || walFile.length() == 0) return;
        LOGGER.info("[RemoteData] Replaying WAL from " + walFile.getAbsolutePath());
        int replayed = 0;
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(walFile)))) {
            while (dis.available() > 0) {
                int magic = dis.readInt();
                if (magic != WAL_MAGIC) {
                    LOGGER.warning("[RemoteData] WAL corrupted at entry " + replayed + " – stopping replay");
                    break;
                }
                int keyLen = dis.readShort() & 0xFFFF;
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                int dataLen = dis.readInt();
                byte[] data = new byte[dataLen];
                dis.readFully(data);
                putRam(key, data, true);
                writeDisk(key, data);
                dirtyQueue.offer(key);
                replayed++;
            }
        } catch (EOFException ignored) {
            // partial last entry – fine
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Error replaying WAL", e);
        }
        LOGGER.info("[RemoteData] WAL replay complete. Recovered " + replayed + " entries.");
        // Truncate WAL after successful replay
        try { new FileOutputStream(walFile, false).close(); } catch (IOException ignored) {}
    }

    private void closeWAL() {
        synchronized (walLock) {
            if (walOut != null) {
                try { walOut.close(); } catch (IOException ignored) {}
                walOut = null;
            }
        }
        try { new FileOutputStream(walFile, false).close(); } catch (IOException ignored) {}
    }
}
