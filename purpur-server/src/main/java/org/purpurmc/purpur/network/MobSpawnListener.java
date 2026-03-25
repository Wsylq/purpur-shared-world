package org.purpurmc.purpur.network;

import org.bukkit.entity.Mob;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.CreatureSpawnEvent;

import java.util.logging.Logger;

/**
 * SLAVE-side listener: cancels all natural mob spawning on slaves.
 *
 * Mobs must only exist on slaves as ghost copies pushed from master.
 * This prevents double-spawning and ensures master is the sole AI authority.
 *
 * Allows spawns caused by commands or plugins (SpawnReason.CUSTOM) so that
 * admins can still force-spawn things if needed.
 *
 * Registered only on SLAVE.
 */
public final class MobSpawnListener implements Listener {

    private static final Logger LOGGER = Logger.getLogger("MobSpawnListener");

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onCreatureSpawn(CreatureSpawnEvent event) {
        // Allow ghost mobs spawned by MobSyncHandler itself
        if (event.getEntity().hasMetadata("slave_ghost_mob")) return;

        // Allow custom / plugin-triggered spawns (commands, spawn-eggs held by players, etc.)
        CreatureSpawnEvent.SpawnReason reason = event.getSpawnReason();
        if (reason == CreatureSpawnEvent.SpawnReason.CUSTOM
                || reason == CreatureSpawnEvent.SpawnReason.COMMAND) {
            return;
        }

        // Cancel everything else (natural, chunk-gen, spawner, etc.)
        event.setCancelled(true);
        LOGGER.finest("[MobSync] Cancelled slave spawn: " + event.getEntityType()
                + " reason=" + reason);
    }
}
