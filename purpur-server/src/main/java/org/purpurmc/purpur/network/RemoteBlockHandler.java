package org.purpurmc.purpur.network;

import net.minecraft.server.MinecraftServer;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.data.BlockData;

import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteBlockHandler – real-time single-block sync (v3).
 *
 * MASTER mode:
 *   applyPush(BlockSyncMessage) – applies an incoming block action to master's world.
 *   Called from RemoteDataServer after conflict-guard passes.
 *
 * SLAVE mode:
 *   sendAction(world, x, y, z, action, block) – encodes a BlockSyncMessage and sends
 *   it to master via RemoteDataClient.sendBlockAction().
 *
 *   applyPush(BlockSyncMessage) – applies a BLOCK_PUSH received from master.
 *   Schedules on the main thread via MinecraftServer.execute().
 *
 * Slaves cancel local block events (see RemoteBlockListener) and ONLY render
 * what master broadcasts — master is the single source of truth.
 */
public final class RemoteBlockHandler {

    private static final Logger LOGGER = Logger.getLogger("RemoteBlockHandler");

    private RemoteBlockHandler() {}

    // ── Slave send path ───────────────────────────────────────────────────────

    /**
     * Called from RemoteBlockListener on SLAVE servers.
     * Encodes the block change as a BlockSyncMessage and fires it to master.
     */
    public static void sendAction(World world, int x, int y, int z,
                                  byte action, Block block) {
        RemoteDataClient client = RemoteDataClient.get();
        if (client == null || !client.isConnected()) return;

        String blockStateStr = (action == BlockSyncMessage.ACTION_BREAK)
                ? "minecraft:air"
                : block.getBlockData().getAsString();

        BlockSyncMessage msg = new BlockSyncMessage(
                action, x, y, z, System.currentTimeMillis(),
                blockStateStr, world.getName());

        client.sendBlockAction(msg);
        LOGGER.fine("[RemoteData] Sent BLOCK_ACTION pos=" + msg.posKey()
                + " action=" + action + " block=" + blockStateStr);
    }

    // ── Apply path (used on MASTER directly, and on SLAVES from BLOCK_PUSH) ──

    /**
     * Apply a BlockSyncMessage to this server's world.
     * Schedules on the main thread.  Safe to call from any thread.
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
                if (!world.isChunkLoaded(chunkX, chunkZ)) {
                    LOGGER.fine("[RemoteData] applyPush: chunk not loaded, skipping pos=" + msg.posKey());
                    return;
                }

                String blockStateStr = (msg.action == BlockSyncMessage.ACTION_BREAK
                        || msg.blockData == null || msg.blockData.isEmpty())
                        ? "minecraft:air"
                        : msg.blockData;

                BlockData newData = Bukkit.createBlockData(blockStateStr);
                Block block = world.getBlockAt(msg.x, msg.y, msg.z);
                block.setBlockData(newData, false); // false = no physics cascade
                LOGGER.fine("[RemoteData] Applied block pos=" + msg.posKey() + " state=" + blockStateStr);

            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "[RemoteData] applyPush error pos=" + msg.posKey(), e);
            }
        });
    }

    // ── Legacy key-based overload (used by RemoteDataClient BLOCK_PUSH path) ─

    /**
     * Legacy overload kept for compatibility with the old BLOCK_PUSH path.
     * key = "world/x/y/z", data = blockStateString bytes.
     */
    public static void applyPush(String key, byte[] data) {
        String[] parts = key.split("/");
        if (parts.length != 4) {
            LOGGER.warning("[RemoteData] applyPush: bad key: " + key);
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
            LOGGER.warning("[RemoteData] applyPush: bad coords in key: " + key);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    public static String buildKey(String worldName, int x, int y, int z) {
        return worldName + "/" + x + "/" + y + "/" + z;
    }
}
