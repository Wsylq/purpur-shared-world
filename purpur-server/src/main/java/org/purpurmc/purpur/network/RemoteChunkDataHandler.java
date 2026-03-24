package org.purpurmc.purpur.network;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.core.BlockPos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Shared-world chunk bridge.
 *
 * This class stores and loads chunk NBT via RemoteDataCache using stable chunk keys:
 * chunk/<world>/<x>/<z>
 */
public final class RemoteChunkDataHandler {

    private static final Logger LOGGER = Logger.getLogger("RemoteChunkDataHandler");

    private RemoteChunkDataHandler() {
    }

    private static boolean isEnabled() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        return cfg != null && cfg.enabled;
    }

    private static String normalizeWorld(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "world";
        }

        String w = raw.trim();
        int colon = w.indexOf(':');
        if (colon >= 0 && colon + 1 < w.length()) {
            w = w.substring(colon + 1);
        }

        String lower = w.toLowerCase(Locale.ROOT);
        if (lower.equals("overworld") || lower.equals("world")) {
            return "world";
        }
        if (lower.equals("the_nether") || lower.equals("world_nether") || lower.equals("dim-1")) {
            return "world_nether";
        }
        if (lower.equals("the_end") || lower.equals("world_the_end") || lower.equals("dim1")) {
            return "world_the_end";
        }
        return w;
    }

    private static String buildKey(String worldName, ChunkPos pos) {
        return "chunk/" + normalizeWorld(worldName) + "/" + pos.x + "/" + pos.z;
    }

    private static byte[] tagToBytes(CompoundTag tag) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
        NbtIo.writeCompressed(tag, out);
        return out.toByteArray();
    }

    private static CompoundTag bytesToTag(byte[] data) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        return NbtIo.readCompressed(in, net.minecraft.nbt.NbtAccounter.unlimitedHeap());
    }

    public static void save(String worldName, ChunkPos pos, CompoundTag tag) {
        if (!isEnabled()) return;

        try {
            final RemoteDataCache cache = RemoteDataCache.get();
            if (cache == null) return;

            final String key = buildKey(worldName, pos);
            final byte[] bytes = tagToBytes(tag);

            LOGGER.info("[RemoteData] SAVE key=" + key);
            cache.put(key, bytes);
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk save failed for " + worldName + " " + pos, ex);
        }
    }

    public static CompoundTag load(String worldName, ChunkPos pos) {
        if (!isEnabled()) return null;

        try {
            final RemoteDataCache cache = RemoteDataCache.get();
            if (cache == null) return null;

            final String primaryKey = buildKey(worldName, pos);
            byte[] bytes = cache.get(primaryKey);

            if (bytes == null) {
                final String[] aliases = new String[] {
                    "world", "overworld", "minecraft:overworld",
                    "world_nether", "the_nether", "minecraft:the_nether", "DIM-1",
                    "world_the_end", "the_end", "minecraft:the_end", "DIM1"
                };

                for (String alias : aliases) {
                    String key = "chunk/" + normalizeWorld(alias) + "/" + pos.x + "/" + pos.z;
                    bytes = cache.get(key);
                    if (bytes != null) {
                        break;
                    }
                }
            }

            LOGGER.info("[RemoteData] LOAD key=" + primaryKey + " hit=" + (bytes != null));
            return bytes == null ? null : bytesToTag(bytes);
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk load failed for " + worldName + " " + pos, ex);
            return null;
        }
    }

    /**
     * Called by client push path (master -> client) when chunk data is pushed.
     * This updates remote cache; safe chunk hot-reload is intentionally not forced here.
     */
    public static void applyPush(String key, byte[] bytes) {
        if (!isEnabled()) return;
        try {
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.put(key, bytes);
            }
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "[RemoteData] Failed to apply pushed chunk for key=" + key, ex);
        }
    }

    /**
     * If you have a ServerLevel hook calling this, keep it non-blocking.
     * Main-thread network I/O is intentionally avoided.
     */
    public static void onBlockChanged(ServerLevel level, BlockPos pos) {
        // No-op by default. Use chunk-save path hooks for stability.
    }
}
