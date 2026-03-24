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
 * Key format: "chunk/<worldName>/<x>/<z>"
 *
 * ── How this is hooked into Minecraft ────────────────────────────────────────
 *
 * The hook is in RegionFileStorage.java.patch:
 *
 *   write(ChunkPos pos, CompoundTag tag):
 *     RemoteChunkDataHandler.save(worldName, pos, tag);   ← called BEFORE super.write()
 *     super.write(pos, tag);                              ← local disk write still happens
 *
 *   read(ChunkPos pos):
 *     CompoundTag remote = RemoteChunkDataHandler.load(worldName, pos);
 *     if (remote != null) return remote;                  ← use master data if available
 *     return super.read(pos);                             ← fall back to local disk
 *
 * ── Strategy ──────────────────────────────────────────────────────────────────
 *
 * WRITE → immediately PUT to master (async virtual thread), also marks local
 *         cache dirty as a fallback if the connection is temporarily down.
 *
 * READ  → try master first (RAM cache → live master GET), return null to fall
 *         back to Minecraft's own local RegionFile read if nothing found.
 *
 * PUSH  → when the master broadcasts a CHUNK_PUSH to this client,
 *         applyPush() writes data to local cache AND force-unloads the chunk
 *         on the main thread so the in-memory LevelChunk is refreshed
 *         immediately using Bukkit's stable World.unloadChunk() API.
 */
public final class RemoteChunkDataHandler {

    private static final Logger LOGGER = Logger.getLogger("RemoteChunkDataHandler");
    private static final String KEY_PREFIX = "chunk/";

    private RemoteChunkDataHandler() {}

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Save chunk NBT. Called from the RegionFileStorage patch BEFORE the
     * local disk write. Immediately PUTs to master on a virtual thread so
     * other servers receive the data without waiting for the flush interval.
     *
     * @param worldName dimension folder name (e.g. "world", "world_nether")
     * @param pos       ChunkPos
     * @param tag       chunk CompoundTag
     */
    public static void save(String worldName, ChunkPos pos, CompoundTag tag) {
        if (!isEnabled()) return;
        try {
            String key   = buildKey(worldName, pos);
            byte[] bytes = tagToBytes(tag);

            // Write into local cache immediately (dirty = true as fallback)
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.put(key, bytes);
            }

            // Push to master immediately and asynchronously.
            // The virtual thread does not block the Minecraft save thread.
            RemoteDataClient client = RemoteDataClient.get();
            if (client != null && client.isConnected()) {
                final RemoteDataCache cacheRef = cache;
                Thread.ofVirtual().name("RemoteData-Push-" + key).start(() -> {
                    boolean ok = client.put(key, bytes);
                    if (ok && cacheRef != null) {
                        cacheRef.markClean(key); // master has it; no need to flush again
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk save failed: " + worldName + " " + pos, e);
        }
    }

    /**
     * Overload that accepts a ServerLevel directly.
     * Derives the world name via the stable Bukkit API.
     */
    public static void save(ServerLevel level, ChunkPos pos, CompoundTag tag) {
        save(levelToWorldName(level), pos, tag);
    }

    /**
     * Load chunk NBT. Called from the RegionFileStorage patch BEFORE the
     * local disk read. Always tries the master first so this server always
     * gets the freshest copy of the chunk.
     *
     * Returns null if nothing is found remotely → Minecraft falls back to its
     * own local RegionFile read.
     *
     * @param worldName dimension folder name (e.g. "world", "world_nether")
     * @param pos       ChunkPos
     * @return CompoundTag from master, or null if not found remotely
     */
    public static CompoundTag load(String worldName, ChunkPos pos) {
        if (!isEnabled()) return null;
        try {
            String key = buildKey(worldName, pos);

            // 1. Try live master connection for the freshest copy.
            RemoteDataClient client = RemoteDataClient.get();
            if (client != null && client.isConnected()) {
                byte[] bytes = client.get(key);
                if (bytes != null) {
                    // Warm the local cache with the received data
                    RemoteDataCache cache = RemoteDataCache.get();
                    if (cache != null) cache.putClean(key, bytes);
                    return bytesToTag(bytes);
                }
            }

            // 2. Master unavailable / not found – try local RAM cache.
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                byte[] bytes = cache.get(key);
                if (bytes != null) return bytesToTag(bytes);
            }

            // 3. Nothing found remotely → return null (Minecraft uses local disk).
            return null;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Chunk load failed: " + worldName + " " + pos, e);
            return null;
        }
    }

    /**
     * Called by RemoteDataClient when the master broadcasts a CHUNK_PUSH.
     *
     * 1. Writes the fresh data into the local cache.
     * 2. Schedules a chunk force-unload on the main server thread so the
     *    in-memory LevelChunk is dropped and will be re-read (via our patched
     *    RegionFileStorage.read → load() above) next time a player loads it.
     *
     * Uses only the stable Bukkit API (World.unloadChunk) — no private NMS.
     */
    public static void applyPush(String key, byte[] bytes) {
        if (!isEnabled()) return;
        try {
            // 1. Write into local cache (clean – no need to push back to master)
            RemoteDataCache cache = RemoteDataCache.get();
            if (cache != null) {
                cache.putClean(key, bytes);
            }

            // 2. Parse key: "chunk/<worldName>/<x>/<z>"
            ChunkPos pos       = parseChunkPos(key);
            String   worldName = parseWorldName(key);
            if (pos == null || worldName == null) return;

            MinecraftServer server = MinecraftServer.getServer();
            if (server == null) return;

            // 3. Schedule on the main server thread – chunk API is not thread-safe
            server.execute(() -> {
                try {
                    // Match the world by its folder/name via the stable Bukkit API
                    for (ServerLevel level : server.getAllLevels()) {
                        if (!levelToWorldName(level).equals(worldName)) continue;

                        org.bukkit.World bukkitWorld = level.getWorld();
                        if (bukkitWorld == null) break;

                        if (bukkitWorld.isChunkLoaded(pos.x, pos.z)) {
                            // save=false: discard in-memory copy, keep our cache data intact.
                            // Next load will go through patched read() → load() → gets fresh data.
                            bukkitWorld.unloadChunk(pos.x, pos.z, false);
                            LOGGER.fine("[RemoteData] Unloaded chunk for push: " + key);
                        }
                        break;
                    }
                } catch (Exception ex) {
                    LOGGER.log(Level.WARNING, "[RemoteData] applyPush main-thread unload failed: " + key, ex);
                }
            });

        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "[RemoteData] applyPush failed for key=" + key, ex);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static boolean isEnabled() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        return cfg != null && cfg.enabled;
    }

    /**
     * Derive a stable world-folder name from a ServerLevel.
     *
     * Uses the Bukkit World.getName() API which always returns the actual
     * folder name regardless of the Minecraft version.
     *
     * Falls back to parsing the dimension key string for edge cases where
     * getWorld() returns null (e.g. very early in startup).
     */
    static String levelToWorldName(ServerLevel level) {
        // Primary: Bukkit API – always gives the exact folder name
        try {
            org.bukkit.World world = level.getWorld();
            if (world != null) {
                return world.getName();
            }
        } catch (Exception ignored) {}

        // Fallback: parse the dimension ResourceKey toString()
        // ResourceKey.toString() gives something like "ResourceKey[minecraft:level / minecraft:overworld]"
        try {
            String dimStr = level.dimension().toString();
            if (dimStr.contains("overworld"))  return "world";
            if (dimStr.contains("the_nether")) return "world_nether";
            if (dimStr.contains("the_end"))    return "world_the_end";
        } catch (Exception ignored) {}

        return "world"; // safe default
    }

    private static String buildKey(String worldName, ChunkPos pos) {
        return KEY_PREFIX + worldName + "/" + pos.x + "/" + pos.z;
    }

    /** Parse the world name from a key like "chunk/world_nether/4/-9" */
    static String parseWorldName(String key) {
        // key = "chunk/<worldName>/<x>/<z>"
        // worldName itself may contain underscores but never slashes.
        if (!key.startsWith(KEY_PREFIX)) return null;
        String rest = key.substring(KEY_PREFIX.length()); // "<worldName>/<x>/<z>"
        int firstSlash = rest.indexOf('/');
        if (firstSlash < 0) return null;
        return rest.substring(0, firstSlash);
    }

    /** Parse ChunkPos from a key like "chunk/world/4/-9" */
    static ChunkPos parseChunkPos(String key) {
        String[] parts = key.split("/");
        // parts[0]="chunk", parts[1]=worldName, parts[2]=x, parts[3]=z
        if (parts.length < 4) return null;
        try {
            int x = Integer.parseInt(parts[parts.length - 2]);
            int z = Integer.parseInt(parts[parts.length - 1]);
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
