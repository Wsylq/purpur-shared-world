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
 *   "player/<uuid>"           → stored as  storage/player/<uuid>.dat
 *   "chunk/<world>/<x>/<z>"   → stored as  storage/chunk/<world>/<x>/<z>.dat
 *   "cfg/<filename>"          → stored as  storage/cfg/<filename>
 *   "plugin/<name>/<path>"    → stored as  storage/plugin/<name>/<path>
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
        LOGGER.info("[RemoteData] Storage root: " + storageRoot.getAbsolutePath());
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Get raw bytes for a key. Returns null if not found.
     */
    public byte[] get(String key) {
        File f = toFile(key);
        ReadWriteLock lock = lockFor(key);
        lock.readLock().lock();
        try {
            if (!f.exists()) return null;
            return Files.readAllBytes(f.toPath());
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Storage GET failed: " + key, e);
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Store raw bytes for a key, creating parent directories as needed.
     */
    public void put(String key, byte[] data) {
        File f = toFile(key);
        ReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            f.getParentFile().mkdirs();
            Files.write(f.toPath(), data,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Storage PUT failed: " + key, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Delete a key. No-op if it doesn't exist.
     */
    public void delete(String key) {
        File f = toFile(key);
        ReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            if (f.exists()) f.delete();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * List all keys under a given prefix.
     * Prefix "" lists everything; "player/" lists all player keys, etc.
     */
    public String[] list(String prefix) {
        // Convert prefix to a directory
        File dir;
        if (prefix == null || prefix.isEmpty()) {
            dir = storageRoot;
        } else {
            // Strip trailing slash if any
            String p = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
            dir = new File(storageRoot, p.replace('/', File.separatorChar));
        }

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
        // Normalise key to a safe relative path
        String safePath = key.replace('/', File.separatorChar);
        // Prevent path traversal
        File candidate = new File(storageRoot, safePath);
        if (!candidate.getAbsolutePath().startsWith(storageRoot.getAbsolutePath())) {
            throw new SecurityException("Path traversal attempt detected in key: " + key);
        }
        // Append .dat extension if the key doesn't already have one
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
                // Build relative key from path
                String rel = root.toURI().relativize(child.toURI()).getPath();
                // Remove .dat extension
                if (rel.endsWith(".dat")) rel = rel.substring(0, rel.length() - 4);
                // Convert file separators back to /
                out.add(rel.replace(File.separatorChar, '/'));
            }
        }
    }
}
