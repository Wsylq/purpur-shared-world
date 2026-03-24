package org.purpurmc.purpur.network;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.nbt.NbtAccounter;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.ChunkPos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteChunkDataHandler – chunk-level storage sync (SECONDARY path).
 *
 * PURPOSE:
 *   Ensures that when a player logs in or moves into a chunk that was modified
 *   on another server, they load the correct version from master's storage.
 *   This is NOT the real-time sync path — use RemoteBlockListener/RemoteBlockHandler
 *   for instant block-by-block propagation.
 *
 * HOW IT HOOKS IN:
 *   RegionFileStorage.java.patch:
 *     write(ChunkPos, CompoundTag) → RemoteChunkDataHandler.save() BEFORE super.write()
 *     read(ChunkPos)               → RemoteChunkDataHandler.load() BEFORE super.read()
 *
 * WRITE:
 *   On every Minecraft chunk save (autosave, unload, shutdown), the full chunk NBT
 *   is serialised and PUT to master on a virtual thread.  This gives master an
 *   up-to-date snapshot even between block events (e.g. entity data, tile entities).
 *
 * READ:
 *   On every chunk load, master is queried first.  If master has data, it's used
 *   and the local disk copy is bypassed.  This ensures a server that has been
 *   offline for a while never serves stale data.
 *
 * applyPush (CHUNK_PUSH from master):
 *   Writes received data to local cache only.
 *   Does NOT unload the chunk — that caused a deadlock (sync chunk load on main thread).
 *   The chunk will naturally reload from the updated cache the next time it is needed.
 *   Real-time visible block changes are handled by RemoteBlockHandler.applyPush().
 */
public final class RemoteChunkDataHandler {

    private static final Logger LOGGER = Logger.getLogger("RemoteChunkDataHandler");
    private static final String KEY_PREFIX = "chunk/";

    private RemoteChunkDataHandler() {}

    // ── Save (called from RegionFileStorage patch) ────────────────────────────

    /**
     * Called BEFORE the local disk write on every chunk save.
     * Immediately PUTs chunk data to master on a virtual thread.
     */
    public static void save(String worldName, ChunkPos pos, CompoundTag tag) {
        if (!isEnabled()) return;
        try {
            String key   = buildKey(worldName, pos);
            byte[] bytes = tagToBytes(tag);

            // Write to local cache as dirty fallback
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) cache.put(key, bytes);

            // Push to master immediately on a virtual thread
            RemoteDataClient client = RemoteDataClient.get();
            if (client != null && client.isConnected()) {
                final RemoteDataCache cacheRef = cache;
                Thread.ofVirtual().name("RemoteData-ChunkSave-" + key).start(() -> {
                    boolean ok = client.put(key, bytes);
                    if (ok && cacheRef != null) cacheRef.markClean(key);
                });
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk save failed: " + worldName + " " + pos, e);
        }
    }

    public static void save(ServerLevel level, ChunkPos pos, CompoundTag tag) {
        save(levelToWorldName(level), pos, tag);
    }

    // ── Load (called from RegionFileStorage patch) ────────────────────────────

    /**
     * Called BEFORE the local disk read on every chunk load.
     * Returns master's copy if available; null = fall back to local disk.
     */
    public static CompoundTag load(String worldName, ChunkPos pos) {
        if (!isEnabled()) return null;
        try {
            String key = buildKey(worldName, pos);

            // 1. Try live master connection (freshest data)
            RemoteDataClient client = RemoteDataClient.get();
            if (client != null && client.isConnected()) {
                byte[] bytes = client.get(key);
                if (bytes != null) {
                    RemoteDataCache cache = RemoteDataCache.get();
                    if (cache != null) cache.putClean(key, bytes);
                    return bytesToTag(bytes);
                }
            }

            // 2. Fall back to local RAM/disk cache
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                byte[] bytes = cache.get(key);
                if (bytes != null) return bytesToTag(bytes);
            }

            // 3. Nothing found remotely — Minecraft uses local disk
            return null;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk load failed: " + worldName + " " + pos, e);
            return null;
        }
    }

    // ── Apply push (CHUNK_PUSH from master → this client) ────────────────────

    /**
     * Called when master broadcasts a CHUNK_PUSH (full chunk NBT update).
     *
     * Writes data to local cache only.
     * Does NOT unload/reload the chunk — that caused a deadlock.
     * Real-time block visibility is handled by RemoteBlockHandler.applyPush().
     * The updated chunk data will be used the next time the chunk is naturally
     * loaded (player enters area, server restart, etc.).
     */
    public static void applyPush(String key, byte[] bytes) {
        if (!isEnabled()) return;
        try {
            // Write to local cache only (no chunk unload, no main-thread call)
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) cache.putClean(key, bytes);
            LOGGER.fine("[RemoteData] CHUNK_PUSH cached: " + key + " (" + bytes.length + " bytes)");
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] applyPush failed for key=" + key, e);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static boolean isEnabled() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        return cfg != null && cfg.enabled;
    }

    static String levelToWorldName(ServerLevel level) {
        try {
            org.bukkit.World world = level.getWorld();
            if (world != null) return world.getName();
        } catch (Exception ignored) {}
        try {
            String dimStr = level.dimension().toString();
            if (dimStr.contains("overworld"))  return "world";
            if (dimStr.contains("the_nether")) return "world_nether";
            if (dimStr.contains("the_end"))    return "world_the_end";
        } catch (Exception ignored) {}
        return "world";
    }

    private static String buildKey(String worldName, ChunkPos pos) {
        return KEY_PREFIX + worldName + "/" + pos.x + "/" + pos.z;
    }

    static String parseWorldName(String key) {
        if (!key.startsWith(KEY_PREFIX)) return null;
        String rest = key.substring(KEY_PREFIX.length());
        int slash = rest.indexOf('/');
        return slash < 0 ? null : rest.substring(0, slash);
    }

    static ChunkPos parseChunkPos(String key) {
        String[] parts = key.split("/");
        if (parts.length < 4) return null;
        try {
            int x = Integer.parseInt(parts[parts.length - 2]);
            int z = Integer.parseInt(parts[parts.length - 1]);
            return new ChunkPos(x, z);
        } catch (NumberFormatException e) { return null; }
    }

    private static byte[] tagToBytes(CompoundTag tag) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(8192);
        NbtIo.writeCompressed(tag, bos);
        return bos.toByteArray();
    }

    private static CompoundTag bytesToTag(byte[] bytes) throws IOException {
        return NbtIo.readCompressed(new ByteArrayInputStream(bytes), NbtAccounter.unlimitedHeap());
    }
}
