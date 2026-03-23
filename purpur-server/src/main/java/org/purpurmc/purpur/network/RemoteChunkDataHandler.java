package org.purpurmc.purpur.network;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.world.level.ChunkPos;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteChunkDataHandler
 *
 * Intercepts chunk NBT save/load so chunk data flows through the remote
 * cache/master system.
 *
 * Key format: "chunk/<worldName>/<chunkX>/<chunkZ>"
 *
 * Strategy:
 *  READ  → RAM cache → disk cache → master → null (Minecraft generates fresh chunk)
 *  WRITE → RAM cache (immediate, non-blocking) + WAL → background flush to master
 *
 * How to hook this in:
 * Patch net.minecraft.world.level.chunk.storage.RegionFileStorage (Paper-mapped):
 *   - read(ChunkPos)  → call RemoteChunkDataHandler.load(worldName, pos)
 *   - write(ChunkPos, CompoundTag) → call RemoteChunkDataHandler.save(worldName, pos, tag)
 *
 * The patch still calls the original RegionFile methods so data is also
 * written locally (double-write = safety net).
 */
public class RemoteChunkDataHandler {

    private static final Logger LOGGER = Logger.getLogger("RemoteChunkDataHandler");
    private static final String KEY_PREFIX = "chunk/";

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Save chunk NBT. Non-blocking from MC thread perspective;
     * actual master sync happens on background flush thread.
     *
     * @param worldName  e.g. "world", "world_nether", "world_the_end"
     * @param pos        ChunkPos
     * @param tag        the chunk CompoundTag
     */
    public static void save(String worldName, ChunkPos pos, CompoundTag tag) {
        if (!isEnabled()) return;
        try {
            String key = buildKey(worldName, pos);
            byte[] bytes = tagToBytes(tag);
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.put(key, bytes);  // dirty → will be flushed async
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "[RemoteData] Chunk save failed: " + worldName + " " + pos, e);
        }
    }

    /**
     * Load chunk NBT. Returns null if not available remotely
     * (Minecraft will fall back to local RegionFile).
     *
     * @param worldName  dimension folder name
     * @param pos        ChunkPos
     * @return CompoundTag or null
     */
    public static CompoundTag load(String worldName, ChunkPos pos) {
        if (!isEnabled()) return null;
        try {
            String key = buildKey(worldName, pos);
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache == null) return null;

            byte[] bytes = cache.get(key);

            if (bytes == null) {
                // Cache miss – try master
                RemoteDataClient client = RemoteDataClient.get();
                if (client != null && client.isConnected()) {
                    bytes = client.get(key);
                    if (bytes != null) {
                        cache.putClean(key, bytes);
                    }
                }
            }

            if (bytes == null) return null;
            return bytesToTag(bytes);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "[RemoteData] Chunk load failed: " + worldName + " " + pos, e);
            return null;
        }
    }

    /**
     * Invalidate a chunk from cache (e.g. on world reset / void fill).
     */
    public static void invalidate(String worldName, ChunkPos pos) {
        if (!isEnabled()) return;
        String key = buildKey(worldName, pos);
        RemoteDataCache cache = RemoteDataCache.get();
        if (cache != null) cache.invalidate(key);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String buildKey(String worldName, ChunkPos pos) {
        return KEY_PREFIX + worldName + "/" + pos.x + "/" + pos.z;
    }

    private static boolean isEnabled() {
        RemoteDataConfig cfg;
        try { cfg = RemoteDataConfig.get(); }
        catch (IllegalStateException e) { return false; }
        return cfg.enabled;
    }

    private static byte[] tagToBytes(CompoundTag tag) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(8192);
        NbtIo.writeCompressed(tag, bos);
        return bos.toByteArray();
    }

    private static CompoundTag bytesToTag(byte[] bytes) throws IOException {
        return NbtIo.readCompressed(new ByteArrayInputStream(bytes),
                net.minecraft.nbt.NbtAccounter.unlimitedHeap());
    }
}
