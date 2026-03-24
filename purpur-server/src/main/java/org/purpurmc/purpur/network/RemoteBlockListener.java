package org.purpurmc.purpur.network;

import org.bukkit.block.Block;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.*;
import org.bukkit.event.entity.EntityChangeBlockEvent;
import org.bukkit.event.entity.EntityExplodeEvent;
import org.bukkit.event.player.PlayerBucketEmptyEvent;
import org.bukkit.event.player.PlayerBucketFillEvent;
import org.bukkit.event.world.StructureGrowEvent;

/**
 * RemoteBlockListener – Bukkit event listener (v3, master-slave).
 *
 * SLAVE mode (isWorldOwner = false):
 *   • Cancels all block-changing events at LOWEST priority.
 *   • At MONITOR priority it forwards the intended change to master via
 *     RemoteBlockHandler.sendAction().
 *   • Master applies & broadcasts the authoritative state back.
 *
 * MASTER mode (isWorldOwner = true):
 *   • Does NOT cancel events — the master's world is the source of truth.
 *   • At MONITOR priority it broadcasts the resulting block state to all slaves
 *     via RemoteBlockHandler's broadcastBlockPush path (through RemoteDataServer).
 *   • This is registered only in CLIENT (slave) mode by RemoteDataManager.
 *     On master the events naturally run and chunk-save keeps storage in sync.
 *
 * Registration:
 *   RemoteDataManager.init() registers this listener only on SLAVE servers.
 *   Master servers do not need it — their block changes are applied locally
 *   and then broadcast by RemoteDataServer after receiving BLOCK_ACTION.
 */
public class RemoteBlockListener implements Listener {

    // ── Cancel events on slave BEFORE they touch the world ───────────────────
    // LOWEST priority runs first — we cancel here so the world is never mutated.

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBlockPlace(BlockPlaceEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
        // Forward intent to master
        Block b = event.getBlock();
        RemoteBlockHandler.sendAction(b.getWorld(), b.getX(), b.getY(), b.getZ(),
                BlockSyncMessage.ACTION_PLACE, b);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBlockBreak(BlockBreakEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
        Block b = event.getBlock();
        RemoteBlockHandler.sendAction(b.getWorld(), b.getX(), b.getY(), b.getZ(),
                BlockSyncMessage.ACTION_BREAK, b);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBucketEmpty(PlayerBucketEmptyEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
        Block b = event.getBlock();
        RemoteBlockHandler.sendAction(b.getWorld(), b.getX(), b.getY(), b.getZ(),
                BlockSyncMessage.ACTION_PLACE, b);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBucketFill(PlayerBucketFillEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
        Block b = event.getBlock();
        RemoteBlockHandler.sendAction(b.getWorld(), b.getX(), b.getY(), b.getZ(),
                BlockSyncMessage.ACTION_BREAK, b);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBlockBurn(BlockBurnEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBlockFade(BlockFadeEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBlockForm(BlockFormEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBlockGrow(BlockGrowEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBlockSpread(BlockSpreadEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelBlockExplode(BlockExplodeEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelEntityExplode(EntityExplodeEvent event) {
        if (!isSlaveMode()) return;
        // Don't cancel entity explosion itself, but clear the block list
        event.blockList().clear();
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelEntityChangeBlock(EntityChangeBlockEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelStructureGrow(StructureGrowEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelPistonExtend(BlockPistonExtendEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void cancelPistonRetract(BlockPistonRetractEvent event) {
        if (!isSlaveMode()) return;
        event.setCancelled(true);
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private static boolean isSlaveMode() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        return cfg != null && cfg.enabled && !cfg.isWorldOwner;
    }
}
