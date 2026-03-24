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
 *   Tier 1 – RAM  (ConcurrentHashMap with LRU eviction)
 *   Tier 2 – Disk (flat key→file under ./remote-cache/)
 *
 * FIX FOR DOUBLE-SEND / TIMEOUT BUG:
 * ────────────────────────────────────
 * Previously RemoteChunkDataHandler.save() fired an immediate virtual-thread PUT
 * AND the dirty-flush background thread also found the same key in dirtyKeys() and
 * sent a SECOND PUT.  Both blocked on synchronized(dos) simultaneously — one of
 * them always timed out.
 *
 * Fix: the flush thread now delegates to RemoteDataClient.put(), which tracks
 * in-flight puts via its inFlightPuts map.  If a virtual-thread push is already
 * in progress for a key, the flush thread's call to client.put() will simply
 * await the virtual thread's result instead of firing a duplicate wire write.
 *
 * Additionally the flush thread only processes keys that are still dirty AND
 * not being pushed by the immediate path — this is guaranteed by RemoteDataClient
 * returning the same Future, but we also skip keys where markClean() has already
 * been called by the virtual thread.
 *
 * Write-Ahead Log (WAL): every write is appended before being acknowledged.
 * On startup the WAL is replayed so no writes are lost even if the server
 * crashes before a flush.
 */
public class RemoteDataCache {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataCache");

    // ── Singleton ────────────────────────────────────────────────────────────
    private static RemoteDataCache INSTANCE;
    public static RemoteDataCache get()                                            { return INSTANCE; }
    public static RemoteDataCache init(RemoteDataConfig config, File serverRoot)   {
        INSTANCE = new RemoteDataCache(config, serverRoot);
        return INSTANCE;
    }

    // ── LRU RAM cache entry ──────────────────────────────────────────────────
    private static class Entry {
        volatile byte[]   data;
        volatile boolean  dirty;
        volatile long     lastAccessNs;

        Entry(byte[] data, boolean dirty) {
            this.data         = data;
            this.dirty        = dirty;
            this.lastAccessNs = System.nanoTime();
        }
        void touch() { lastAccessNs = System.nanoTime(); }
    }

    // ── Fields ───────────────────────────────────────────────────────────────
    private final RemoteDataConfig config;
    private final File             cacheDir;
    private final File             walFile;

    private final ConcurrentHashMap<String, Entry>  ramCache   = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String>     dirtyQueue = new ConcurrentLinkedQueue<>();

    private DataOutputStream walOut;
    private final Object     walLock  = new Object();

    private final AtomicLong hits   = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();

    private volatile boolean running = false;
    private Thread flushThread;

    // ── Constructor ──────────────────────────────────────────────────────────
    private RemoteDataCache(RemoteDataConfig config, File serverRoot) {
        this.config   = config;
        this.cacheDir = new File(serverRoot, "remote-cache");
        this.walFile  = new File(serverRoot, config.walPath);
        cacheDir.mkdirs();
        if (config.walEnabled) {
            openWAL();
            replayWAL();
        }
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /**
     * Get data for key.
     * Order: RAM → disk → null (caller fetches from master directly).
     */
    public byte[] get(String key) {
        Entry e = ramCache.get(key);
        if (e != null) { e.touch(); hits.incrementAndGet(); return e.data; }

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
     *
     * The background flush thread will push to master as a retry/fallback.
     * The primary push is done immediately by RemoteChunkDataHandler.save().
     * RemoteDataClient.put() deduplicates concurrent pushes for the same key.
     */
    public void put(String key, byte[] data) {
        if (config.walEnabled) appendWAL(key, data);
        putRam(key, data, true);
        writeDisk(key, data);
        dirtyQueue.offer(key);
        evictIfNeeded();
    }

    /**
     * Write data directly to RAM+disk WITHOUT marking dirty.
     * Used when data just arrived from the master (no need to re-push).
     */
    public void putClean(String key, byte[] data) {
        putRam(key, data, false);
        writeDisk(key, data);
    }

    /** Remove from cache (RAM + disk).  Does NOT delete on master. */
    public void invalidate(String key) {
        ramCache.remove(key);
        deleteDisk(key);
    }

    /** Returns all keys that are still dirty (for fallback batch sync). */
    public Set<String> dirtyKeys() {
        Set<String> keys = new HashSet<>();
        ramCache.forEach((k, e) -> { if (e.dirty) keys.add(k); });
        return keys;
    }

    /** Mark a key as no longer dirty (after successful push to master). */
    public void markClean(String key) {
        Entry e = ramCache.get(key);
        if (e != null) e.dirty = false;
    }

    /** Peek at cached data without altering dirty state. */
    public byte[] peek(String key) {
        Entry e = ramCache.get(key);
        return e != null ? e.data : null;
    }

    // ── Background flush thread ──────────────────────────────────────────────

    public void start() {
        running     = true;
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
     *
     * FIX: This is now purely a RETRY / FALLBACK path.
     * The primary push happens immediately in RemoteChunkDataHandler.save() on a
     * virtual thread.  The flush thread calling client.put() for the same key will
     * be deduplicated by RemoteDataClient.inFlightPuts — it simply awaits the
     * virtual thread's result instead of sending a duplicate packet.
     *
     * Keys that have already been marked clean (markClean called by the virtual
     * thread after a successful immediate push) are skipped entirely.
     *
     * @param forceAll flush ALL dirty entries (used on shutdown)
     */
    private void flushDirty(boolean forceAll) {
        RemoteDataClient client = RemoteDataClient.get();
        if (client == null || !client.isConnected()) return;

        List<String>   batch     = new ArrayList<>(config.batchSize);
        List<byte[]>   batchData = new ArrayList<>(config.batchSize);

        Set<String> dirty = dirtyKeys();
        for (String key : dirty) {
            Entry e = ramCache.get(key);
            // Skip if already cleaned by the immediate push thread
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

    /**
     * Send a batch of dirty entries to master.
     *
     * FIX: client.put() now deduplicates concurrent pushes internally, so calling
     * it here for a key that is being pushed by a virtual thread simply awaits the
     * virtual thread's result — no duplicate packet is ever written to the wire.
     */
    private void sendBatch(RemoteDataClient client, List<String> keys, List<byte[]> dataList) {
        for (int i = 0; i < keys.size(); i++) {
            String key  = keys.get(i);
            byte[] data = dataList.get(i);
            if (data == null) continue;

            // Re-check: skip if already marked clean by the immediate push thread
            Entry e = ramCache.get(key);
            if (e != null && !e.dirty) continue;

            boolean ok = client.put(key, data);
            if (ok) {
                markClean(key);
                LOGGER.fine("[RemoteData] Flushed key=" + key + " (" + data.length + " bytes)");
            } else {
                LOGGER.warning("[RemoteData] Flush failed for key=" + key + " (will retry)");
            }
        }
    }

    // ── RAM cache helpers ────────────────────────────────────────────────────

    private void putRam(String key, byte[] data, boolean dirty) {
        Entry e = ramCache.get(key);
        if (e != null) {
            e.data  = data;
            e.dirty = dirty || e.dirty; // don't clear dirty on re-put
            e.touch();
        } else {
            ramCache.put(key, new Entry(data, dirty));
        }
    }

    private void evictIfNeeded() {
        int max = config.maxChunkCacheEntries;
        if (ramCache.size() <= max) return;

        // Evict the LRU clean entry
        ramCache.entrySet().stream()
            .filter(me -> !me.getValue().dirty
                    && me.getValue().lastAccessNs < System.nanoTime() - 60_000_000_000L)
            .min(Comparator.comparingLong(me -> me.getValue().lastAccessNs))
            .ifPresent(me -> ramCache.remove(me.getKey()));
    }

    // ── Disk helpers ─────────────────────────────────────────────────────────

    private File keyToFile(String key) {
        String safe = key.replace('/', File.separatorChar);
        return new File(cacheDir, safe + ".bin");
    }

    private void writeDisk(String key, byte[] data) {
        File f = keyToFile(key);
        f.getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(f)) {
            fos.write(data);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Disk write failed for key=" + key, e);
        }
    }

    private byte[] readDisk(String key) {
        File f = keyToFile(key);
        if (!f.exists()) return null;
        try {
            return Files.readAllBytes(f.toPath());
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Disk read failed for key=" + key, e);
            return null;
        }
    }

    private void deleteDisk(String key) {
        keyToFile(key).delete();
    }

    // ── WAL ──────────────────────────────────────────────────────────────────

    private static final int WAL_MAGIC = 0xAB_CD_01_02;

    private void openWAL() {
        synchronized (walLock) {
            try {
                walOut = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(walFile, true), 65536));
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "[RemoteData] Cannot open WAL file", e);
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
                LOGGER.log(Level.WARNING, "[RemoteData] WAL write error", e);
            }
        }
    }

    private void replayWAL() {
        if (!walFile.exists() || walFile.length() == 0) return;
        int replayed = 0;
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(walFile), 65536))) {
            while (dis.available() > 0) {
                int magic = dis.readInt();
                if (magic != WAL_MAGIC) {
                    LOGGER.warning("[RemoteData] WAL corrupted at entry " + replayed + " – stopping replay");
                    break;
                }
                int    keyLen   = dis.readShort() & 0xFFFF;
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);
                String key     = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                int    dataLen = dis.readInt();
                byte[] data    = new byte[dataLen];
                dis.readFully(data);
                putRam(key, data, true);
                writeDisk(key, data);
                dirtyQueue.offer(key);
                replayed++;
            }
        } catch (EOFException ignored) {
            // partial last entry — normal after a crash
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
