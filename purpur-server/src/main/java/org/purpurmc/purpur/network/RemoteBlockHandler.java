package org.purpurmc.purpur.network;

import net.minecraft.server.MinecraftServer;
import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.data.BlockData;

import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteBlockHandler – real-time single-block sync (v4, fixed).
 *
 * SLAVE sendAction():
 *   Called from RemoteBlockListener on slave servers.
 *   Accepts the already-resolved blockStateStr (not derived from block.getBlockData()
 *   post-cancel, which was the v3 bug).
 *
 * applyPush():
 *   Applies a BlockSyncMessage to this server's world on the main thread.
 *   If the chunk is not loaded it is force-loaded synchronously (safe on main thread).
 *   Physics suppressed (false) to avoid cascading updates on slave displays.
 */
public final class RemoteBlockHandler {

    private static final Logger LOGGER = Logger.getLogger("RemoteBlockHandler");

    private RemoteBlockHandler() {}

    // ── Slave send path ───────────────────────────────────────────────────────

    /**
     * Called from RemoteBlockListener on SLAVE servers.
     * blockStateStr must already be resolved by the caller (avoids the
     * post-cancel getBlockData() ambiguity that existed in v3).
     */
    public static void sendAction(World world, int x, int y, int z,
                                  byte action, String blockStateStr) {
        RemoteDataClient client = RemoteDataClient.get();
        if (client == null || !client.isConnected()) {
            LOGGER.fine("[RemoteData] sendAction skipped – not connected to master");
            return;
        }
        BlockSyncMessage msg = new BlockSyncMessage(
                action, x, y, z, System.currentTimeMillis(),
                blockStateStr, world.getName());
        client.sendBlockAction(msg);
        LOGGER.fine("[RemoteData] Sent BLOCK_ACTION " + msg);
    }

    /**
     * Overload kept for compatibility. Derives blockStateStr from block.
     * NOTE: Only safe to call BEFORE the event is cancelled; pass the Block
     * reference whose state reflects what the player intends to place/break.
     */
    public static void sendAction(World world, int x, int y, int z,
                                  byte action, Block block) {
        String blockStateStr = (action == BlockSyncMessage.ACTION_BREAK)
                ? "minecraft:air"
                : block.getBlockData().getAsString();
        sendAction(world, x, y, z, action, blockStateStr);
    }

    // ── Apply path ────────────────────────────────────────────────────────────

    /**
     * Apply a BlockSyncMessage to this server's world.
     * Must be called (or rescheduled) on the server main thread.
     * If the chunk containing the block is not loaded it will be force-loaded
     * so the update is never silently dropped.
     */
    public static void applyPush(BlockSyncMessage msg) {
        MinecraftServer server = MinecraftServer.getServer();
        if (server == null) return;

        server.execute(() -> {
            try {
                World world = Bukkit.getWorld(msg.worldName);
                if (world == null) {
                    LOGGER.fine("[RemoteData] applyPush: world not found: " + msg.worldName);
                    return;
                }

                int chunkX = msg.x >> 4;
                int chunkZ = msg.z >> 4;

                // Force-load if not loaded so the update is never dropped
                if (!world.isChunkLoaded(chunkX, chunkZ)) {
                    world.loadChunk(chunkX, chunkZ, false); // false = don't generate new chunks
                    if (!world.isChunkLoaded(chunkX, chunkZ)) {
                        // Chunk doesn't exist (ungenerated area) – skip
                        LOGGER.fine("[RemoteData] applyPush: chunk ungenerated, skipping pos=" + msg.posKey());
                        return;
                    }
                }

                String blockStateStr = (msg.action == BlockSyncMessage.ACTION_BREAK
                        || msg.blockData == null || msg.blockData.isBlank())
                        ? "minecraft:air"
                        : msg.blockData;

                BlockData newData;
                try {
                    newData = Bukkit.createBlockData(blockStateStr);
                } catch (IllegalArgumentException e) {
                    LOGGER.warning("[RemoteData] applyPush: invalid blockstate '" + blockStateStr
                            + "' at pos=" + msg.posKey() + " – defaulting to air");
                    newData = Bukkit.createBlockData(Material.AIR);
                }

                Block block = world.getBlockAt(msg.x, msg.y, msg.z);
                block.setBlockData(newData, false); // false = suppress physics cascade on display servers

                LOGGER.fine("[RemoteData] Applied " + msg);

            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "[RemoteData] applyPush error pos=" + msg.posKey(), e);
            }
        });
    }

    // ── Legacy key-based overload ─────────────────────────────────────────────

    /**
     * Legacy overload: key = "worldName/x/y/z", data = blockState UTF-8 bytes.
     * Kept for compatibility with old BLOCK_PUSH handling path.
     */
    public static void applyPush(String key, byte[] data) {
        String[] parts = key.split("/");
        if (parts.length != 4) {
            LOGGER.warning("[RemoteData] applyPush legacy: bad key: " + key);
            return;
        }
        try {
            String worldName     = parts[0];
            int    x             = Integer.parseInt(parts[1]);
            int    y             = Integer.parseInt(parts[2]);
            int    z             = Integer.parseInt(parts[3]);
            String blockStateStr = (data == null || data.length == 0)
                    ? "minecraft:air"
                    : new String(data, StandardCharsets.UTF_8);
            BlockSyncMessage msg = new BlockSyncMessage(
                    BlockSyncMessage.ACTION_PLACE, x, y, z,
                    System.currentTimeMillis(), blockStateStr, worldName);
            applyPush(msg);
        } catch (NumberFormatException e) {
            LOGGER.warning("[RemoteData] applyPush legacy: bad coords in key: " + key);
        }
    }

    // ── Key helper ────────────────────────────────────────────────────────────

    public static String buildKey(String worldName, int x, int y, int z) {
        return worldName + "/" + x + "/" + y + "/" + z;
    }
}
