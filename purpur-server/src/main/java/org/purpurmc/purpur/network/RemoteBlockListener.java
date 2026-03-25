package org.purpurmc.purpur.network;

import org.bukkit.Material;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.block.data.BlockData;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.*;
import org.bukkit.event.entity.EntityChangeBlockEvent;
import org.bukkit.event.entity.EntityExplodeEvent;
import org.bukkit.event.player.PlayerBucketEmptyEvent;
import org.bukkit.event.player.PlayerBucketFillEvent;
import org.bukkit.event.world.StructureGrowEvent;
import org.bukkit.inventory.ItemStack;

import java.util.logging.Logger;

/**
 * RemoteBlockListener – Bukkit event listener (v4, master-slave, fixed).
 *
 * ── SLAVE mode (isWorldOwner = false) ──────────────────────────────────────
 *   LOWEST priority: cancel the event so the world is NEVER mutated locally.
 *   Read the intended block state from event.getBlockPlaced() (Paper/Purpur API
 *   guarantees this returns the to-be-placed BlockState even when cancelled).
 *   Forward to master via RemoteBlockHandler.sendAction(world, x, y, z, action, stateStr).
 *
 * ── MASTER mode (isWorldOwner = true) ──────────────────────────────────────
 *   MONITOR priority (after all plugins): if NOT cancelled, broadcast the
 *   resulting block state to all slaves via RemoteDataServer.broadcastBlockPushPublic().
 *
 * ── Registration ────────────────────────────────────────────────────────────
 *   RemoteDataManager.init() registers this on BOTH master and slave.
 *   Mode is determined at runtime via isSlaveMode() / isMasterMode().
 */
public class RemoteBlockListener implements Listener {

    private static final Logger LOGGER = Logger.getLogger("RemoteBlockListener");

    // ════════════════════════════════════════════════════════════════════════
    // SLAVE – LOWEST priority: cancel & forward intent to master
    // ════════════════════════════════════════════════════════════════════════

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = false)
    public void onBlockPlace_slave(BlockPlaceEvent event) {
        if (!isSlaveMode()) return;
        // getBlockPlaced() returns the block in its NEW (to-be-placed) state.
        // This is safe to read before setCancelled(true) — Paper/Purpur sets it
        // when constructing the event, not when applying to world.
        Block placed = event.getBlockPlaced();
        String blockStateStr = resolveBlockStateStr(placed, event.getItemInHand());
        event.setCancelled(true);
        RemoteBlockHandler.sendAction(placed.getWorld(), placed.getX(), placed.getY(), placed.getZ(),
                BlockSyncMessage.ACTION_PLACE, blockStateStr);
    }

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = false)
    public void onBlockMultiPlace_slave(BlockMultiPlaceEvent event) {
        if (!isSlaveMode()) return;
        // Multi-place: beds, doors, tall plants — cancel and forward each sub-block
        event.setCancelled(true);
        for (BlockState newState : event.getReplacedBlockStates()) {
            // getReplacedBlockStates gives OLD states; we need the new ones.
            // Access the actual block after cancel is irrelevant — use newState location
            // and derive from the item since we cancelled before apply.
            // Safest: use the placed block's item data for all sub-blocks.
        }
        // Simpler: just send the primary placed block; master will handle the full structure
        Block placed = event.getBlockPlaced();
        String blockStateStr = resolveBlockStateStr(placed, event.getItemInHand());
        RemoteBlockHandler.sendAction(placed.getWorld(), placed.getX(), placed.getY(), placed.getZ(),
                BlockSyncMessage.ACTION_PLACE, blockStateStr);
    }

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = false)
    public void onBlockBreak_slave(BlockBreakEvent event) {
        if (!isSlaveMode()) return;
        Block b = event.getBlock();
        event.setCancelled(true);
        RemoteBlockHandler.sendAction(b.getWorld(), b.getX(), b.getY(), b.getZ(),
                BlockSyncMessage.ACTION_BREAK, "minecraft:air");
    }

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = false)
    public void onBucketEmpty_slave(PlayerBucketEmptyEvent event) {
        if (!isSlaveMode()) return;
        Block b = event.getBlock();
        event.setCancelled(true);
        String blockStateStr = bucketToBlock(event.getBucket());
        RemoteBlockHandler.sendAction(b.getWorld(), b.getX(), b.getY(), b.getZ(),
                BlockSyncMessage.ACTION_PLACE, blockStateStr);
    }

    @EventHandler(priority = EventPriority.LOWEST, ignoreCancelled = false)
    public void onBucketFill_slave(PlayerBucketFillEvent event) {
        if (!isSlaveMode()) return;
        Block b = event.getBlock();
        event.setCancelled(true);
        RemoteBlockHandler.sendAction(b.getWorld(), b.getX(), b.getY(), b.getZ(),
                BlockSyncMessage.ACTION_BREAK, "minecraft:air");
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onBlockBurn_slave(BlockBurnEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onBlockFade_slave(BlockFadeEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onBlockForm_slave(BlockFormEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onBlockGrow_slave(BlockGrowEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onBlockSpread_slave(BlockSpreadEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onBlockExplode_slave(BlockExplodeEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onEntityExplode_slave(EntityExplodeEvent event) {
        if (!isSlaveMode()) return;
        event.blockList().clear(); // allow entity death; just suppress block damage
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onEntityChangeBlock_slave(EntityChangeBlockEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onStructureGrow_slave(StructureGrowEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onPistonExtend_slave(BlockPistonExtendEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onPistonRetract_slave(BlockPistonRetractEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    // ════════════════════════════════════════════════════════════════════════
    // MASTER – MONITOR priority: broadcast resulting state to all slaves
    // ═══════════════════════════════════════════���════════════════════════════

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPlace_master(BlockPlaceEvent event) {
        if (!isMasterMode()) return;
        Block b = event.getBlockPlaced();
        broadcastToSlaves(b, BlockSyncMessage.ACTION_PLACE, b.getBlockData().getAsString());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockMultiPlace_master(BlockMultiPlaceEvent event) {
        if (!isMasterMode()) return;
        // Broadcast each sub-block of multi-place (doors, beds, etc.)
        // At MONITOR priority after commit, block states are final.
        Block primary = event.getBlockPlaced();
        broadcastToSlaves(primary, BlockSyncMessage.ACTION_PLACE, primary.getBlockData().getAsString());
        // The secondary block (top half of door, etc.) has already been placed by the server.
        // We broadcast the primary; slaves will see both halves if they re-load the chunk,
        // or we can query adjacent blocks. For simplicity send primary only:
        // advanced: iterate event.getReplacedBlockStates() positions and read current world state.
        for (BlockState oldState : event.getReplacedBlockStates()) {
            Block actual = oldState.getWorld().getBlockAt(oldState.getLocation());
            if (!actual.getType().isAir()) {
                broadcastToSlaves(actual, BlockSyncMessage.ACTION_PLACE, actual.getBlockData().getAsString());
            }
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBreak_master(BlockBreakEvent event) {
        if (!isMasterMode()) return;
        Block b = event.getBlock();
        broadcastToSlaves(b, BlockSyncMessage.ACTION_BREAK, "minecraft:air");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBucketEmpty_master(PlayerBucketEmptyEvent event) {
        if (!isMasterMode()) return;
        Block b = event.getBlock();
        broadcastToSlaves(b, BlockSyncMessage.ACTION_PLACE, bucketToBlock(event.getBucket()));
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBucketFill_master(PlayerBucketFillEvent event) {
        if (!isMasterMode()) return;
        Block b = event.getBlock();
        broadcastToSlaves(b, BlockSyncMessage.ACTION_BREAK, "minecraft:air");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBurn_master(BlockBurnEvent event) {
        if (!isMasterMode()) return;
        Block b = event.getBlock();
        broadcastToSlaves(b, BlockSyncMessage.ACTION_BREAK, "minecraft:air");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockFade_master(BlockFadeEvent event) {
        if (!isMasterMode()) return;
        // After fade, read the new state
        Block b = event.getBlock();
        broadcastToSlaves(b, BlockSyncMessage.ACTION_PLACE, b.getBlockData().getAsString());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityChangeBlock_master(EntityChangeBlockEvent event) {
        if (!isMasterMode()) return;
        Block b = event.getBlock();
        // Schedule 1 tick so the physical change has been applied before we read
        safeRunNextTick(() ->
            broadcastToSlaves(b, BlockSyncMessage.ACTION_PLACE, b.getBlockData().getAsString()));
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPistonExtend_master(BlockPistonExtendEvent event) {
        if (!isMasterMode()) return;
        // Piston physics resolve next tick
        java.util.List<Block> moved = new java.util.ArrayList<>(event.getBlocks());
        Block pistonBlock = event.getBlock();
        safeRunNextTick(() -> {
            for (Block b : moved) {
                broadcastToSlaves(b, BlockSyncMessage.ACTION_PLACE, b.getBlockData().getAsString());
            }
            broadcastToSlaves(pistonBlock, BlockSyncMessage.ACTION_PLACE,
                    pistonBlock.getBlockData().getAsString());
        });
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPistonRetract_master(BlockPistonRetractEvent event) {
        if (!isMasterMode()) return;
        java.util.List<Block> moved = new java.util.ArrayList<>(event.getBlocks());
        Block pistonBlock = event.getBlock();
        safeRunNextTick(() -> {
            for (Block b : moved) {
                broadcastToSlaves(b, BlockSyncMessage.ACTION_PLACE, b.getBlockData().getAsString());
            }
            broadcastToSlaves(pistonBlock, BlockSyncMessage.ACTION_PLACE,
                    pistonBlock.getBlockData().getAsString());
        });
    }

    // ════════════════════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Resolve the block state string for a PLACE action.
     * Uses block.getBlockData() first; falls back to item-derived data if the
     * block reports AIR (can happen on some server implementations when reading
     * post-cancel at LOWEST priority for certain block types).
     */
    private static String resolveBlockStateStr(Block placed, ItemStack itemInHand) {
        BlockData bd = placed.getBlockData();
        if (bd.getMaterial() != Material.AIR) {
            return bd.getAsString();
        }
        // Fallback: derive from held item
        if (itemInHand != null && itemInHand.getType().isBlock()) {
            try {
                return itemInHand.getType().createBlockData().getAsString();
            } catch (Exception ignored) {}
        }
        return "minecraft:air";
    }

    private static void broadcastToSlaves(Block b, byte action, String blockStateStr) {
        RemoteDataServer server = RemoteDataServer.get();
        if (server == null) return;
        BlockSyncMessage msg = new BlockSyncMessage(
                action, b.getX(), b.getY(), b.getZ(),
                System.currentTimeMillis(), blockStateStr, b.getWorld().getName());
        server.broadcastBlockPushPublic(msg);
        LOGGER.fine("[RemoteData] Master broadcast " + msg);
    }

    private static String bucketToBlock(Material bucket) {
        if (bucket == null) return "minecraft:water[level=0]";
        return switch (bucket) {
            case WATER_BUCKET       -> "minecraft:water[level=0]";
            case LAVA_BUCKET        -> "minecraft:lava[level=0]";
            case POWDER_SNOW_BUCKET -> "minecraft:powder_snow";
            default                 -> "minecraft:water[level=0]";
        };
    }

    private static boolean isSlaveMode() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        return cfg != null && cfg.enabled && !cfg.isWorldOwner;
    }

    private static boolean isMasterMode() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        return cfg != null && cfg.enabled && cfg.isWorldOwner && cfg.isMaster;
    }

    private static void safeRunNextTick(Runnable task) {
        try {
            org.bukkit.plugin.Plugin[] plugins = org.bukkit.Bukkit.getPluginManager().getPlugins();
            if (plugins.length > 0) {
                org.bukkit.Bukkit.getScheduler().runTask(plugins[0], task);
                return;
            }
        } catch (Exception ignored) {}
        // If no plugin, run immediately (slightly less safe for piston, but won't crash)
        task.run();
    }
}
