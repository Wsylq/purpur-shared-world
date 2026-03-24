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
 * RemoteChunkDataHandler
 *
 * Intercepts chunk NBT save/load so chunk data flows through the remote
 * cache/master system.
 *
 * Key format:  "chunk/<worldName>/<chunkX>/<chunkZ>"
 *
 * ── How this is hooked into Minecraft ──────────────────────────────────────
 * Patch net.minecraft.world.level.chunk.storage.RegionFileStorage (Paper mapped):
 *
 *   write(ChunkPos pos, CompoundTag tag):
 *       RemoteChunkDataHandler.save(worldName, pos, tag);
 *       // then call original write so data is also kept on local disk
 *
 *   read(ChunkPos pos):
 *       CompoundTag remote = RemoteChunkDataHandler.load(worldName, pos);
 *       if (remote != null) return remote;
 *       // fall through to original disk read
 *
 * The patch must be added to:
 *   purpur-server/minecraft-patches/sources/
 *       net/minecraft/world/level/chunk/storage/RegionFileStorage.java.patch
 *
 * ── Strategy ────────────────────────────────────────────────────────────────
 *  WRITE → immediately PUT to master (async via client thread), also marks
 *          local cache dirty as a fallback if client is temporarily disconnected.
 *
 *  READ  → try master first (via RAM cache → disk cache → live master GET),
 *          return null only when nothing is found so Minecraft falls back to
 *          its own local RegionFile read.
 *
 *  PUSH  → when the master broadcasts a CHUNK_PUSH to this client,
 *          applyPush() writes the data to local cache AND schedules a
 *          chunk reload on the server's main thread so the in-memory
 *          LevelChunk is refreshed immediately.
 */
public final class RemoteChunkDataHandler {

    private static final Logger LOGGER = Logger.getLogger("RemoteChunkDataHandler");
    private static final String KEY_PREFIX = "chunk/";

    private RemoteChunkDataHandler() {}

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * FIX #1 & #2 (combined):
     *
     * Save chunk NBT. Immediately PUTs to master synchronously on a virtual
     * thread (non-blocking from MC thread's perspective). This replaces the
     * old lazy 5-second flush which caused the master to never receive data
     * in time for a same-session sync.
     *
     * The local cache is also updated so reads from this server itself are
     * consistent.
     *
     * @param level     the ServerLevel (used to derive a stable world name)
     * @param pos       ChunkPos
     * @param tag       the chunk CompoundTag to save
     */
    public static void save(ServerLevel level, ChunkPos pos, CompoundTag tag) {
        if (!isEnabled()) return;
        try {
            String worldName = levelToWorldName(level);
            String key = buildKey(worldName, pos);
            byte[] bytes = tagToBytes(tag);

            // Write into local cache immediately (dirty flag set)
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.put(key, bytes);
            }

            // FIX #2: Push to master immediately and asynchronously,
            // do NOT wait for the 5-second flush thread.
            RemoteDataClient client = RemoteDataClient.get();
            if (client != null && client.isConnected()) {
                Thread.ofVirtual().name("RemoteData-ImmediatePush").start(() -> {
                    boolean ok = client.put(key, bytes);
                    if (ok && cache != null) {
                        cache.markClean(key); // already on master; no need to flush again
                    }
                    LOGGER.info("[RemoteData] SAVE key=" + key);
                });
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk save failed for " + pos, e);
        }
    }

    /**
     * Convenience overload that accepts the world name as a string directly.
     * Used when called from a RegionFileStorage patch that already has the
     * folder name available.
     */
    public static void save(String worldName, ChunkPos pos, CompoundTag tag) {
        if (!isEnabled()) return;
        try {
            String key = buildKey(worldName, pos);
            byte[] bytes = tagToBytes(tag);

            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.put(key, bytes);
            }

            RemoteDataClient client = RemoteDataClient.get();
            if (client != null && client.isConnected()) {
                Thread.ofVirtual().name("RemoteData-ImmediatePush").start(() -> {
                    boolean ok = client.put(key, bytes);
                    if (ok && cache != null) {
                        cache.markClean(key);
                    }
                    LOGGER.info("[RemoteData] SAVE key=" + key);
                });
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk save failed: " + worldName + " " + pos, e);
        }
    }

    /**
     * FIX #4: Load chunk NBT. Always tries the master first (via local RAM
     * cache → disk cache → live master GET).
     *
     * The old code returned local cache first, meaning a chunk present on
     * this server's own disk was never overridden by the master's copy.
     *
     * Returns null if nothing is found remotely – Minecraft will then fall
     * back to reading from its own local RegionFile.
     *
     * @param worldName dimension folder name (e.g. "world", "world_nether")
     * @param pos       ChunkPos
     * @return CompoundTag, or null if not found remotely
     */
    public static CompoundTag load(String worldName, ChunkPos pos) {
        if (!isEnabled()) return null;
        try {
            String key = buildKey(worldName, pos);
            RemoteDataCache cache = RemoteDataCache.get();

            // FIX #4: Try master/network first, THEN fall back to local cache.
            // Step 1: check live master connection for the freshest copy.
            RemoteDataClient client = RemoteDataClient.get();
            if (client != null && client.isConnected()) {
                byte[] bytes = client.get(key);
                if (bytes != null) {
                    if (cache != null) cache.putClean(key, bytes); // warm local cache
                    return bytesToTag(bytes);
                }
            }

            // Step 2: master unavailable / not found – try local RAM cache.
            if (cache != null) {
                byte[] bytes = cache.get(key);
                if (bytes != null) return bytesToTag(bytes);
            }

            // Step 3: nothing found remotely → return null (Minecraft uses local disk).
            return null;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk load failed: " + worldName + " " + pos, e);
            return null;
        }
    }

    /**
     * FIX #3 (applyPush fix):
     *
     * Called by RemoteDataClient when the master broadcasts a CHUNK_PUSH.
     * Writes data to local cache AND schedules a chunk reload on the main
     * thread so the in-memory LevelChunk on this server reflects the changes
     * that another server just saved.
     *
     * Previously this only updated the cache (dead code path) and never
     * triggered a reload, so chunks appeared unchanged on other servers.
     */
    public static void applyPush(String key, byte[] bytes) {
        if (!isEnabled()) return;
        try {
            // 1. Write into local cache (clean – no need to re-push to master)
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.putClean(key, bytes);
            }

            // 2. FIX #3: Parse the key and schedule a chunk reload on the main thread.
            // Key format: chunk/<world>/<x>/<z>
            ChunkPos pos = parseChunkPos(key);
            String worldName = parseWorldName(key);
            if (pos == null || worldName == null) return;

            MinecraftServer server = MinecraftServer.getServer();
            if (server == null) return;

            // Schedule on the main server thread to avoid concurrency issues
            server.execute(() -> {
                for (ServerLevel level : server.getAllLevels()) {
                    if (!levelToWorldName(level).equals(worldName)) continue;

                    // If chunk is currently loaded in this server's memory, unload it
                    // so it gets re-read from the (now updated) cache on next access.
                    long chunkKey = pos.toLong();
                    if (level.getChunkSource().isChunkLoaded(pos.x, pos.z)) {
                        // Force the chunk to be saved-then-dropped so next load
                        // goes through our patched RegionFileStorage.read() → load().
                        level.getChunkSource().chunkMap.scheduleUnload(chunkKey,
                                level.getChunkSource().chunkMap.updatingChunkMap.get(chunkKey));
                        LOGGER.info("[RemoteData] Scheduled chunk reload for key=" + key);
                    }
                    break;
                }
            });

        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "[RemoteData] applyPush failed for key=" + key, ex);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static boolean isEnabled() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        return cfg != null && cfg.enabled; // FIX #6: get() now returns null instead of throwing
    }

    /**
     * FIX #5: Derive world name from the level's dimension path instead of
     * using the buggy normalizeWorld() that had a broken arithmetic expression.
     *
     * Produces: "world" (overworld), "world_nether", "world_the_end"
     * matching the default Minecraft folder naming.
     */
    static String levelToWorldName(ServerLevel level) {
        String dimPath = level.dimension().location().getPath(); // e.g. "overworld", "the_nether", "the_end"
        return switch (dimPath) {
            case "overworld" -> "world";
            case "the_nether" -> "world_nether";
            case "the_end"   -> "world_the_end";
            default          -> dimPath; // custom dimensions: use path as-is
        };
    }

    private static String buildKey(String worldName, ChunkPos pos) {
        return KEY_PREFIX + worldName + "/" + pos.x + "/" + pos.z;
    }

    /** Parse the world name segment from a chunk key like "chunk/world/4/-9" */
    private static String parseWorldName(String key) {
        // key = "chunk/<world>/<x>/<z>"
        String[] parts = key.split("/");
        if (parts.length < 4) return null;
        // parts[0]="chunk", parts[1]=world, parts[2]=x, parts[3]=z
        // world name itself may contain underscores but no slashes
        return parts[1];
    }

    /** Parse ChunkPos from a chunk key like "chunk/world/4/-9" */
    private static ChunkPos parseChunkPos(String key) {
        String[] parts = key.split("/");
        if (parts.length < 4) return null;
        try {
            int x = Integer.parseInt(parts[2]);
            int z = Integer.parseInt(parts[3]);
            return new ChunkPos(x, z);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static byte[] tagToBytes(CompoundTag tag) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(8192);
        NbtIo.writeCompressed(tag, bos);
        return bos.toByteArray();
    }

    private static CompoundTag bytesToTag(byte[] bytes) throws IOException {
        return NbtIo.readCompressed(
                new ByteArrayInputStream(bytes),
                NbtAccounter.unlimitedHeap());
    }
}
