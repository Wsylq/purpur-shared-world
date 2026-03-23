package org.purpurmc.purpur.network;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;

import java.io.*;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemotePlayerDataHandler
 *
 * Intercepts player NBT save/load so player data is stored on the master
 * server instead of (or in addition to) the local playerdata/ folder.
 *
 * Key format: "player/<uuid>"
 *
 * How to hook this in:
 * In net.minecraft.world.level.storage.PlayerDataStorage (or equivalent
 * Paper-mapped class), replace the file read/write calls with:
 *
 *   // SAVE:
 *   RemotePlayerDataHandler.save(player.getStringUUID(), nbtTag);
 *
 *   // LOAD:
 *   Optional<CompoundTag> tag = RemotePlayerDataHandler.load(player.getStringUUID());
 *
 * The patch file (0xxx-Remote-Player-Data.patch) wraps these calls around
 * the existing file I/O so local files are still written as a fallback.
 */
public class RemotePlayerDataHandler {

    private static final Logger LOGGER = Logger.getLogger("RemotePlayerDataHandler");
    private static final String KEY_PREFIX = "player/";

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Save player NBT to cache (+ master asynchronously).
     * Also writes the local file as a fallback (caller still does that).
     */
    public static void save(String uuid, CompoundTag tag) {
        if (!isEnabled()) return;
        try {
            byte[] bytes = tagToBytes(tag);
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.put(KEY_PREFIX + uuid, bytes);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Failed to save player data for " + uuid, e);
        }
    }

    /**
     * Load player NBT. Tries remote first, falls back to null (caller reads local file).
     */
    public static Optional<CompoundTag> load(String uuid) {
        if (!isEnabled()) return Optional.empty();
        try {
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache == null) return Optional.empty();

            byte[] bytes = cache.get(KEY_PREFIX + uuid);
            if (bytes == null) {
                // Cache miss – fetch from master
                RemoteDataClient client = RemoteDataClient.get();
                if (client != null && client.isConnected()) {
                    bytes = client.get(KEY_PREFIX + uuid);
                    if (bytes != null) {
                        cache.putClean(KEY_PREFIX + uuid, bytes);
                    }
                }
            }

            if (bytes == null) return Optional.empty();
            CompoundTag tag = bytesToTag(bytes);
            return Optional.ofNullable(tag);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Failed to load player data for " + uuid, e);
            return Optional.empty();
        }
    }

    /**
     * Delete player data from cache and remote (e.g. when a player is banned/deleted).
     */
    public static void delete(String uuid) {
        if (!isEnabled()) return;
        String key = KEY_PREFIX + uuid;
        RemoteDataCache cache = RemoteDataCache.get();
        if (cache != null) cache.invalidate(key);
        RemoteDataClient client = RemoteDataClient.get();
        if (client != null && client.isConnected()) {
            client.delete(key);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static boolean isEnabled() {
        RemoteDataConfig cfg;
        try { cfg = RemoteDataConfig.get(); }
        catch (IllegalStateException e) { return false; }
        return cfg.enabled;
    }

    private static byte[] tagToBytes(CompoundTag tag) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);
        NbtIo.writeCompressed(tag, bos);
        return bos.toByteArray();
    }

    private static CompoundTag bytesToTag(byte[] bytes) throws IOException {
        return NbtIo.readCompressed(new ByteArrayInputStream(bytes),
                net.minecraft.nbt.NbtAccounter.unlimitedHeap());
    }
}
