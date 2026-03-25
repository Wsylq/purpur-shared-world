package org.purpurmc.purpur.network;

import org.bukkit.entity.Mob;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.EntityDamageByEntityEvent;

import java.util.logging.Logger;

/**
 * MASTER-side listener: intercepts damage on mobs and broadcasts
 * an immediate health-update (MobSyncMessage TYPE_HEALTH_UPDATE)
 * to all slaves so they see damage/death in real time.
 *
 * Registered only on MASTER.
 */
public final class MobDamageListener implements Listener {

    private static final Logger LOGGER = Logger.getLogger("MobDamageListener");

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityDamage(EntityDamageByEntityEvent event) {
        // Only care about mobs being damaged (not players, not other entities)
        if (!(event.getEntity() instanceof Mob mob)) return;

        // Don't process ghost mobs (should never be on master, but safety check)
        if (mob.hasMetadata("slave_ghost_mob")) return;

        RemoteDataServer server = RemoteDataServer.get();
        if (server == null || !server.hasSlaves()) return;

        MobSyncBroadcaster broadcaster = MobSyncBroadcaster.get();
        if (broadcaster == null) return;

        String worldName = mob.getWorld().getName();

        // Schedule on next tick so health value is already updated
        org.bukkit.Bukkit.getScheduler().runTask(
                findPlugin(),
                () -> broadcaster.broadcastHealthUpdate(mob, worldName)
        );
    }

    private org.bukkit.plugin.Plugin findPlugin() {
        for (org.bukkit.plugin.Plugin p : org.bukkit.Bukkit.getPluginManager().getPlugins()) {
            if (p.isEnabled()) return p;
        }
        return null;
    }
}
