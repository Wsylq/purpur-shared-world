package org.purpurmc.purpur.network;

import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.data.BlockData;

import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteBlockHandler – real-time single block sync.
 *
 * This is the PRIMARY sync path (v2).
 *
 * WHY THIS EXISTS:
 *   The chunk-based sync only fires when Minecraft saves chunks to disk (every ~5 min).
 *   Players placing blocks see those blocks appear on the other server only after autosave.
 *   This handler intercepts every block place/break/interact event via RemoteBlockListener
 *   and sends a BLOCK_UPDATE packet to master, which immediately broadcasts a BLOCK_PUSH
 *   to all other servers.  The receiving server applies the block change directly to the
 *   live world — no chunk unload/reload, no disk I/O, instant.
 *
 * PACKET FORMAT:
 *   key  = "world/x/y/z"         (e.g. "world/100/64/-200")
 *   data = blockStateString bytes (UTF-8), e.g. "minecraft:oak_log[axis=y]"
 *          Empty/zero-length data means AIR (block was broken).
 *
 * THREAD SAFETY:
 *   sendPush() is called from the main server thread (Bukkit event handlers).
 *   It hands off to RemoteDataClient.sendBlockUpdate() which fires a virtual thread,
 *   so the main thread is never blocked.
 *
 *   applyPush() is called from RemoteDataClient's reader thread.
 *   It schedules the block set via server.execute() (main thread) to satisfy
 *   Bukkit's thread-safety requirement.  No chunk load or disk I/O occurs —
 *   setBlockData() only updates an already-loaded chunk's in-memory state.
 *   If the chunk is not loaded on this server, the update is skipped (the chunk
 *   will be loaded correctly from master's storage when a player approaches).
 */
public final class RemoteBlockHandler {

    private static final Logger LOGGER = Logger.getLogger("RemoteBlockHandler");

    private RemoteBlockHandler() {}

    // ── Send path (Server A, called from Bukkit event listener) ─────────────

    /**
     * Called after a block is placed or broken on this server.
     * Sends a BLOCK_UPDATE to master immediately on a virtual thread.
     *
     * @param world  Bukkit World
     * @param x      block X
     * @param y      block Y
     * @param z      block Z
     * @param block  the Block after the change (get its new state here)
     */
    public static void sendPush(World world, int x, int y, int z, Block block) {
        RemoteDataClient client = RemoteDataClient.get();
        if (client == null || !client.isConnected()) return;

        String key             = buildKey(world.getName(), x, y, z);
        String blockStateStr   = block.getBlockData().getAsString();
        byte[] blockStateBytes = blockStateStr.getBytes(StandardCharsets.UTF_8);

        client.sendBlockUpdate(key, blockStateBytes);
        LOGGER.fine("[RemoteData] Sent BLOCK_UPDATE key=" + key + " state=" + blockStateStr);
    }

    // ── Receive path (Server B, called from RemoteDataClient reader thread) ──

    /**
     * Apply a BLOCK_PUSH received from master.
     * Schedules the block update on the main server thread.
     * Safe to call from any thread.
     *
     * @param key   "world/x/y/z"
     * @param data  blockStateString bytes; empty = AIR
     */
    public static void applyPush(String key, byte[] data) {
        MinecraftServer server = MinecraftServer.getServer();
        if (server == null) return;

        // Parse key
        String[] parts = key.split("/");
        if (parts.length != 4) {
            LOGGER.warning("[RemoteData] BLOCK_PUSH: bad key format: " + key);
            return;
        }
        String worldName = parts[0];
        int x, y, z;
        try {
            x = Integer.parseInt(parts[1]);
            y = Integer.parseInt(parts[2]);
            z = Integer.parseInt(parts[3]);
        } catch (NumberFormatException e) {
            LOGGER.warning("[RemoteData] BLOCK_PUSH: bad coords in key: " + key);
            return;
        }

        String blockStateStr = (data == null || data.length == 0)
                ? "minecraft:air"
                : new String(data, StandardCharsets.UTF_8);

        // Schedule on main thread — block API is not thread-safe
        server.execute(() -> {
            try {
                // Find the matching world
                World world = Bukkit.getWorld(worldName);
                if (world == null) {
                    LOGGER.fine("[RemoteData] BLOCK_PUSH: world not found: " + worldName);
                    return;
                }

                int chunkX = x >> 4;
                int chunkZ = z >> 4;

                // Only apply if the chunk is already loaded — avoids forcing a
                // synchronous chunk load (which caused the previous deadlock crash).
                // If the chunk is not loaded, the player will get the correct data
                // from master's chunk storage when they enter the area.
                if (!world.isChunkLoaded(chunkX, chunkZ)) {
                    LOGGER.fine("[RemoteData] BLOCK_PUSH: chunk not loaded, skipping: " + key);
                    return;
                }

                BlockData newData = Bukkit.createBlockData(blockStateStr);
                Block block = world.getBlockAt(x, y, z);
                block.setBlockData(newData, false); // false = don't apply physics/lighting update
                // Note: physics=false prevents cascading block updates (sand falling, water flowing)
                // that could corrupt world state. The originating server has already handled physics.

                LOGGER.fine("[RemoteData] Applied BLOCK_PUSH key=" + key + " state=" + blockStateStr);

            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "[RemoteData] BLOCK_PUSH apply error for key=" + key, e);
            }
        });
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Build the block key: "worldName/x/y/z"
     * Uses the Bukkit world name (which matches the world folder name).
     */
    public static String buildKey(String worldName, int x, int y, int z) {
        return worldName + "/" + x + "/" + y + "/" + z;
    }
}
