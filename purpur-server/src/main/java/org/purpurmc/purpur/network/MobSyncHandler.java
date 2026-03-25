package org.purpurmc.purpur.network;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.Entity;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.LivingEntity;
import org.bukkit.entity.Mob;
import org.bukkit.util.Vector;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SLAVE-side: receives MobSyncMessage pushes from master and applies them
 * to the local world (position, velocity, health). Spawns ghost mobs when
 * they are seen for the first time; removes them when the master marks dead.
 *
 * All world-mutation calls are dispatched to the main thread via
 * Bukkit.getScheduler().runTask().
 */
public final class MobSyncHandler {

    private static final Logger LOGGER = Logger.getLogger("MobSyncHandler");

    /** UUID → last-known timestamp – prevents old packets from overwriting newer state */
    private static final ConcurrentHashMap<UUID, Long> lastSeen = new ConcurrentHashMap<>();

    private MobSyncHandler() {}

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Apply an incoming MOB_PUSH from master.
     * Safe to call from any thread – schedules main-thread work internally.
     */
    public static void applyPush(MobSyncMessage msg) {
        // Reject stale packets
        long prev = lastSeen.getOrDefault(msg.entityUUID, Long.MIN_VALUE);
        if (msg.timestamp < prev) {
            LOGGER.fine("[MobSync] Discarding stale packet for " + msg.entityUUID);
            return;
        }
        lastSeen.put(msg.entityUUID, msg.timestamp);

        // Dispatch to main thread
        org.bukkit.plugin.Plugin plugin = findPlugin();
        if (plugin == null) {
            LOGGER.warning("[MobSync] No plugin found – cannot schedule mob update");
            return;
        }

        Bukkit.getScheduler().runTask(plugin, () -> applyOnMainThread(msg));
    }

    /** Remove all tracked ghost mobs (called on disconnect / shutdown). */
    public static void clearAll() {
        lastSeen.clear();
        // Must run entity removal on the main thread
        org.bukkit.plugin.Plugin plugin = findPlugin();
        if (plugin != null) {
            Bukkit.getScheduler().runTask(plugin, () -> {
                for (World world : Bukkit.getWorlds()) {
                    for (Entity e : world.getEntities()) {
                        if (e.hasMetadata("slave_ghost_mob")) {
                            e.remove();
                        }
                    }
                }
                LOGGER.info("[MobSync] Cleared all ghost mobs.");
            });
        } else {
            // Fallback: if called before any plugin is loaded (e.g. pre-enable), just clear the map
            LOGGER.info("[MobSync] clearAll() called before plugin available – map cleared, entities left for GC.");
        }
    }

    // ── Main-thread application ───────────────────────────────────────────────

    private static void applyOnMainThread(MobSyncMessage msg) {
        try {
            World world = Bukkit.getWorld(msg.worldName);
            if (world == null) {
                LOGGER.fine("[MobSync] World not loaded on slave: " + msg.worldName);
                return;
            }

            // Try to find existing entity by UUID
            Entity entity = world.getEntity(msg.entityUUID);

            if (msg.isDead()) {
                if (entity != null) entity.remove();
                lastSeen.remove(msg.entityUUID);
                return;
            }

            Location targetLoc = new Location(world, msg.x, msg.y, msg.z, msg.yaw, msg.pitch);

            if (entity == null) {
                // Spawn ghost mob – no AI, no drops, invisible to mob-cap
                entity = spawnGhostMob(world, targetLoc, msg);
                if (entity == null) return;
            }

            // Teleport + velocity
            entity.teleport(targetLoc);
            entity.setVelocity(new Vector(msg.velX, msg.velY, msg.velZ));

            // Health
            if (entity instanceof LivingEntity living) {
                living.setMaxHealth(msg.maxHealth);
                double hp = Math.max(0, Math.min(msg.health, msg.maxHealth));
                living.setHealth(hp);
            }

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[MobSync] applyOnMainThread error for " + msg.entityUUID, e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Entity spawnGhostMob(World world, Location loc, MobSyncMessage msg) {
        try {
            EntityType type = resolveEntityType(msg.entityType);
            if (type == null || !type.isAlive()) {
                LOGGER.fine("[MobSync] Unknown/non-living entity type: " + msg.entityType);
                return null;
            }

            // getEntityClass() returns Class<? extends Entity> — we cast to LivingEntity
            // since we already checked isAlive() above.
            Class<? extends org.bukkit.entity.Entity> rawClass = type.getEntityClass();
            if (rawClass == null) {
                LOGGER.fine("[MobSync] No entity class for type: " + msg.entityType);
                return null;
            }
            // Safe: isAlive() guarantees this is a LivingEntity subclass
            Class<? extends LivingEntity> entityClass =
                    (Class<? extends LivingEntity>) rawClass;

            org.bukkit.plugin.Plugin plugin = findPlugin();

            // CRITICAL: use world.spawn(loc, Class, Consumer) so the Consumer runs BEFORE
            // CreatureSpawnEvent fires. That way MobSpawnListener sees slave_ghost_mob=true
            // and allows the spawn instead of cancelling it.
            //
            // Also pass randomizeData=false so the mob doesn't get random equipment/colour/etc.
            LivingEntity e = world.spawn(loc, entityClass, false, entity -> {
                // Tag first — this is what MobSpawnListener checks
                entity.setMetadata("slave_ghost_mob",
                        new org.bukkit.metadata.FixedMetadataValue(plugin, true));
                entity.setMetadata("master_uuid",
                        new org.bukkit.metadata.FixedMetadataValue(plugin, msg.entityUUID.toString()));
                // Disable AI so the mob stands still — master drives movement via teleport
                if (entity instanceof Mob mob) {
                    mob.setAware(false);
                }
                entity.setRemoveWhenFarAway(false);
                // Set health inside consumer so it's applied before the first tick
                entity.setMaxHealth(msg.maxHealth);
                entity.setHealth(Math.max(0.1, msg.health));
            });

            LOGGER.fine("[MobSync] Spawned ghost mob " + msg.entityType + " @ " + loc);
            return e;

        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "[MobSync] Failed to spawn ghost mob: " + msg.entityType, ex);
            return null;
        }
    }

    private static EntityType resolveEntityType(String namespaced) {
        // Strip "minecraft:" prefix if present
        String name = namespaced.contains(":") ? namespaced.split(":")[1] : namespaced;
        try {
            return EntityType.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static org.bukkit.plugin.Plugin findPlugin() {
        try {
            for (org.bukkit.plugin.Plugin p : Bukkit.getPluginManager().getPlugins()) {
                if (p.isEnabled()) return p;
            }
        } catch (Exception ignored) {}
        return null;
    }
}
