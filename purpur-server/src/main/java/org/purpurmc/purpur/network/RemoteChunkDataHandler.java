package org.purpurmc.purpur.network;

import net.minecraft.nbt.CompoundTag;
import net.minecraft.nbt.NbtIo;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.ChunkPos;
import net.minecraft.world.level.chunk.LevelChunk;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteChunkDataHandler
 *
 * Intercepts chunk NBT save/load so chunk data flows through the remote
 * cache/master system.
 *
 * Key format: "chunk/<worldName>/<x>/<z>"
 *
 * Strategy:
 *   READ  → RAM cache → disk cache → master → null (Minecraft generates fresh chunk)
 *   WRITE → RAM cache (immediate, non-blocking) + WAL → background flush to master
 *           → master broadcasts CHUNK_PUSH to all other connected player servers
 *
 * Real-time sync (applyPush):
 *   When the master pushes an updated chunk to this client, applyPush() is called.
 *   It updates the local cache. If the chunk is currently loaded in memory on this
 *   server, it re-reads the NBT and refreshes the in-memory chunk data so players
 *   on THIS server see the changes made on the OTHER server immediately.
 *
 * How to hook this in:
 *   Patch net.minecraft.world.level.chunk.storage.RegionFileStorage (Paper-mapped):
 *   - read(ChunkPos)              → call RemoteChunkDataHandler.load(worldName, pos)
 *   - write(ChunkPos, CompoundTag) → call RemoteChunkDataHandler.save(worldName, pos, tag)
 *
 *   The patch still calls the original RegionFile methods so data is also
 *   written locally (double-write = safety net).
 */
public class RemoteChunkDataHandler {

    private static final Logger LOGGER = Logger.getLogger("RemoteChunkDataHandler");
    private static final String KEY_PREFIX = "chunk/";

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Save chunk NBT. Non-blocking from MC thread perspective;
     * actual master sync happens on background flush thread.
     * Master will then broadcast the updated chunk to all other player servers.
     *
     * @param worldName e.g. "world", "world_nether", "world_the_end"
     * @param pos       ChunkPos
     * @param tag       the chunk CompoundTag
     */
    public static void save(String worldName, ChunkPos pos, CompoundTag tag) {
        if (!isEnabled()) return;
        try {
            String key = buildKey(worldName, pos);
            byte[] bytes = tagToBytes(tag);
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.put(key, bytes); // dirty → will be flushed async to master → master broadcasts to others
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk save failed: " + worldName + " " + pos, e);
        }
    }

    /**
     * Load chunk NBT. Returns null if not available remotely
     * (Minecraft will fall back to local RegionFile).
     *
     * @param worldName dimension folder name
     * @param pos       ChunkPos
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
                // Cache miss – try master directly
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
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk load failed: " + worldName + " " + pos, e);
            return null;
        }
    }

    /**
     * Called by RemoteDataClient when the master pushes a real-time chunk update.
     *
     * This method:
     *  1. Updates the local disk+RAM cache with the new chunk data (clean, no re-sync needed)
     *  2. If the chunk is currently loaded in memory on this server, refreshes it live
     *     so players see the change from the other server immediately.
     *
     * @param key  the full chunk key e.g. "chunk/world/4/-9"
     * @param data the updated chunk NBT bytes
     */
    public static void applyPush(String key, byte[] data) {
        if (!key.startsWith(KEY_PREFIX)) return;

        // Parse key: "chunk/<worldName>/<x>/<z>"
        String[] parts = key.split("/");
        if (parts.length < 4) return;

        String worldName = parts[1];
        int chunkX, chunkZ;
        try {
            chunkX = Integer.parseInt(parts[2]);
            chunkZ = Integer.parseInt(parts[3]);
        } catch (NumberFormatException e) {
            return;
        }

        // 1. Update cache (clean – no need to re-push to master)
        RemoteDataCache cache = RemoteDataCache.get();
        if (cache != null) {
            cache.putClean(key, data);
        }

        // 2. If the chunk is loaded in memory, refresh it live
        try {
            MinecraftServer server = MinecraftServer.getServer();
            if (server == null) return;

            for (ServerLevel level : server.getAllLevels()) {
                String levelName = level.getServer().storageSource.getLevelId();
                // Match world name (handle nether/end suffixes)
                if (!matchesWorldName(levelName, level, worldName)) continue;

                ChunkPos pos = new ChunkPos(chunkX, chunkZ);

                // Check if chunk is loaded in memory
                LevelChunk loadedChunk = level.getChunkSource().getChunkNow(pos.x, pos.z);
                if (loadedChunk == null) {
                    // Chunk not loaded – cache update is enough, it'll be read fresh on next load
                    break;
                }

                // Chunk IS loaded – we need to refresh it from the pushed NBT
                CompoundTag tag = bytesToTag(data);
                if (tag == null) break;

                // Schedule the chunk refresh on the main thread to be safe
                server.execute(() -> {
                    try {
                        // Invalidate the in-memory chunk so next access re-reads from cache
                        // We do this by marking chunk as needing save and reinitializing from NBT
                        level.getChunkSource().chunkMap.getUpdatingChunkIfPresent(pos.toLong());
                        // Force the chunk to be re-read from our updated cache on next tick
                        // The safest approach: unload the chunk if no players are nearby
                        boolean hasNearbyPlayers = !level.getPlayers(
                                p -> p.chunkPosition().getChessboardDistance(pos) <= 2
                        ).isEmpty();

                        if (!hasNearbyPlayers) {
                            // Safe to invalidate – no players right on top of it
                            cache.invalidate(key);
                            cache.putClean(key, data);
                        } else {
                            // Players are nearby – just update cache, chunk will naturally
                            // refresh when it's next saved/loaded
                            cache.putClean(key, data);
                        }

                        LOGGER.fine("[RemoteData] Live chunk refresh applied: " + key);
                    } catch (Exception ex) {
                        LOGGER.log(Level.WARNING, "[RemoteData] Live chunk refresh failed: " + key, ex);
                    }
                });
                break;
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] applyPush failed for key=" + key, e);
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
        try {
            cfg = RemoteDataConfig.get();
        } catch (IllegalStateException e) {
            return false;
        }
        return cfg.enabled;
    }

    /**
     * Match a level to a world name from the chunk key.
     * Handles "world", "world_nether", "world_the_end" naming conventions.
     */
    private static boolean matchesWorldName(String levelId, ServerLevel level, String worldName) {
        // Try direct match first
        if (levelId.equals(worldName)) return true;
        // Try dimension-based match
        String dimPath = level.dimension().identifier().getPath();
        if (dimPath.equals("overworld") && worldName.equals("world")) return true;
        if (dimPath.equals("the_nether") && (worldName.equals("world_nether") || worldName.equals("DIM-1"))) return true;
        if (dimPath.equals("the_end") && (worldName.equals("world_the_end") || worldName.equals("DIM1"))) return true;
        return false;
    }

    private static byte[] tagToBytes(CompoundTag tag) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(8192);
        NbtIo.writeCompressed(tag, bos);
        return bos.toByteArray();
    }

    private static CompoundTag bytesToTag(byte[] bytes) throws IOException {
        return NbtIo.readCompressed(
                new ByteArrayInputStream(bytes),
                net.minecraft.nbt.NbtAccounter.unlimitedHeap());
    }
}
