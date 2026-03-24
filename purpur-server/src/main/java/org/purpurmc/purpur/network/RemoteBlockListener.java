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

import java.util.List;

/**
 * RemoteBlockListener – Bukkit event listener for real-time block sync.
 *
 * This is registered in RemoteDataManager.init() when mode=CLIENT.
 * It listens to every event that changes a block's state and sends
 * a BLOCK_UPDATE to master via RemoteBlockHandler.sendPush().
 *
 * IMPORTANT: All handlers run at MONITOR priority with ignoreCancelled=true
 * so we only sync blocks that were actually changed (not cancelled by other plugins).
 *
 * Events covered:
 *   BlockPlaceEvent          – player places a block
 *   BlockBreakEvent          – player breaks a block
 *   BlockBurnEvent           – fire burns a block
 *   BlockFadeEvent           – snow/ice/coral fading
 *   BlockFormEvent           – snow forming, obsidian forming
 *   BlockGrowEvent           – crops/vines growing
 *   BlockSpreadEvent         – fire spreading, mycelium spreading
 *   BlockExplodeEvent        – TNT/creeper explosion block changes
 *   EntityExplodeEvent       – entity explosion (creeper, etc.)
 *   EntityChangeBlockEvent   – enderman picking/placing, falling sand landing
 *   PlayerBucketEmptyEvent   – placing water/lava
 *   PlayerBucketFillEvent    – picking up water/lava (sets AIR)
 *   StructureGrowEvent       – trees, mushrooms, etc. growing
 *   SignChangeEvent          – sign text changes
 *
 * NOTE: We do NOT cancel or interfere with any events.
 * We only read the final state AFTER the event has been processed.
 */
public class RemoteBlockListener implements Listener {

    // ── Player block interactions ─────────────────────────────────────────────

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPlace(BlockPlaceEvent event) {
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBreak(BlockBreakEvent event) {
        // After break, the block becomes AIR — sendPush reads the block's new state
        // BUT: at MONITOR priority, the block in the world is already changed to AIR.
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    // ── Bucket use ───────────────────────────────────────────────────────────

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBucketEmpty(PlayerBucketEmptyEvent event) {
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBucketFill(PlayerBucketFillEvent event) {
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    // ── Natural block changes ─────────────────────────────────────────────────

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBurn(BlockBurnEvent event) {
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockFade(BlockFadeEvent event) {
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockForm(BlockFormEvent event) {
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockGrow(BlockGrowEvent event) {
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockSpread(BlockSpreadEvent event) {
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    // ── Explosions ────────────────────────────────────────────────────────────

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockExplode(BlockExplodeEvent event) {
        for (Block b : event.blockList()) {
            RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityExplode(EntityExplodeEvent event) {
        for (Block b : event.blockList()) {
            RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
        }
    }

    // ── Entity block changes ──────────────────────────────────────────────────

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityChangeBlock(EntityChangeBlockEvent event) {
        Block b = event.getBlock();
        RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
    }

    // ── Structure growth (trees, mushrooms, bamboo) ───────────────────────────

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onStructureGrow(StructureGrowEvent event) {
        for (var state : event.getBlocks()) {
            Block b = state.getBlock();
            RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
        }
    }

    // ── Piston ───────────────────────────────────────────────────────────────

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPiston(BlockPistonExtendEvent event) {
        // Piston head
        Block piston = event.getBlock();
        RemoteBlockHandler.sendPush(piston.getWorld(), piston.getX(), piston.getY(), piston.getZ(), piston);
        // Moved blocks
        for (Block b : event.getBlocks()) {
            RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPistonRetract(BlockPistonRetractEvent event) {
        Block piston = event.getBlock();
        RemoteBlockHandler.sendPush(piston.getWorld(), piston.getX(), piston.getY(), piston.getZ(), piston);
        for (Block b : event.getBlocks()) {
            RemoteBlockHandler.sendPush(b.getWorld(), b.getX(), b.getY(), b.getZ(), b);
        }
    }
}
