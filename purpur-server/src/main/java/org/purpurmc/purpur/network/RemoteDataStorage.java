package org.purpurmc.purpur.network;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataStorage – flat-file storage backend for the MASTER server.
 *
 * Key naming:
 *   "player/<uuid>"          → stored as storage/player/<uuid>.dat
 *   "chunk/<world>/<x>/<z>" → stored as storage/chunk/<world>/<x>/<z>.dat
 *   "cfg/<name>"             → stored as storage/cfg/<name>
 *   "plugin/<name>/<key>"   → stored as storage/plugin/<name>/<key>
 *
 * Thread-safety: per-key ReadWriteLock striping (256 stripes).
 */
public class RemoteDataStorage {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataStorage");
    private static final int LOCK_STRIPES = 256;

    private final File storageRoot;
    private final ReadWriteLock[] locks;

    public RemoteDataStorage(File storageRoot) {
        this.storageRoot = storageRoot;
        storageRoot.mkdirs();
        locks = new ReadWriteLock[LOCK_STRIPES];
        for (int i = 0; i < LOCK_STRIPES; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
    }

    /**
     * Store data for a key. Creates parent directories as needed.
     */
    public void put(String key, byte[] data) {
        ReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            File f = toFile(key);
            f.getParentFile().mkdirs();
            Files.write(f.toPath(), data,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.SYNC);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Storage write failed for key: " + key, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Retrieve data for a key. Returns null if not found.
     */
    public byte[] get(String key) {
        ReadWriteLock lock = lockFor(key);
        lock.readLock().lock();
        try {
            File f = toFile(key);
            if (!f.exists()) return null;
            return Files.readAllBytes(f.toPath());
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Storage read failed for key: " + key, e);
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Delete a key. No-op if not found.
     */
    public void delete(String key) {
        ReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            File f = toFile(key);
            if (f.exists()) f.delete();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * List all keys that start with the given prefix.
     */
    public String[] list(String prefix) {
        File dir = new File(storageRoot, prefix.replace('/', File.separatorChar));
        List<String> results = new ArrayList<>();
        collectKeys(dir, storageRoot, results);
        return results.toArray(new String[0]);
    }

    /**
     * Check if a key exists.
     */
    public boolean exists(String key) {
        return toFile(key).exists();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private File toFile(String key) {
        String safePath = key.replace('/', File.separatorChar);
        File candidate = new File(storageRoot, safePath);
        if (!candidate.getAbsolutePath().startsWith(storageRoot.getAbsolutePath())) {
            throw new SecurityException("Path traversal attempt detected in key: " + key);
        }
        if (!key.contains(".")) {
            candidate = new File(candidate.getAbsolutePath() + ".dat");
        }
        return candidate;
    }

    private ReadWriteLock lockFor(String key) {
        int stripe = (key.hashCode() & 0x7FFFFFFF) % LOCK_STRIPES;
        return locks[stripe];
    }

    private void collectKeys(File dir, File root, List<String> out) {
        if (dir == null || !dir.exists() || !dir.isDirectory()) return;
        File[] children = dir.listFiles();
        if (children == null) return;
        for (File child : children) {
            if (child.isDirectory()) {
                collectKeys(child, root, out);
            } else {
                String rel = root.toURI().relativize(child.toURI()).getPath();
                if (rel.endsWith(".dat")) rel = rel.substring(0, rel.length() - 4);
                out.add(rel.replace(File.separatorChar, '/'));
            }
        }
    }
}
