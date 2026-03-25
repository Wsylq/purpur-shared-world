package org.purpurmc.purpur.network;

import org.bukkit.Bukkit;
import org.bukkit.entity.Entity;
import org.bukkit.entity.LivingEntity;
import org.bukkit.entity.Mob;
import org.bukkit.entity.Player;
import org.bukkit.util.Vector;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MASTER-side periodic broadcaster.
 *
 * Runs every N ticks (default 4 = ~5 times/sec) on a dedicated virtual thread,
 * iterates all loaded mobs in all worlds, builds a MobSyncMessage for each,
 * and calls RemoteDataServer.broadcastMobPublic().
 *
 * Only runs when there is at least one connected slave.
 */
public final class MobSyncBroadcaster {

    private static final Logger LOGGER = Logger.getLogger("MobSyncBroadcaster");

    private static MobSyncBroadcaster INSTANCE;

    public static MobSyncBroadcaster get()                       { return INSTANCE; }
    public static MobSyncBroadcaster init(RemoteDataConfig cfg)  {
        INSTANCE = new MobSyncBroadcaster(cfg);
        return INSTANCE;
    }

    // ── Config ────────────────────────────────────────────────────────────────

    /** Broadcast interval in server ticks (20 ticks = 1 s). Default 4 → ~5 Hz */
    private final int intervalTicks;

    private volatile boolean running = false;
    private int taskId = -1;

    private MobSyncBroadcaster(RemoteDataConfig cfg) {
        // Read from config if present, else 4 ticks
        this.intervalTicks = 4;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    public void start(org.bukkit.plugin.Plugin plugin) {
        if (running) return;
        running = true;
        taskId = Bukkit.getScheduler().scheduleSyncRepeatingTask(plugin, this::tick, intervalTicks, intervalTicks);
        LOGGER.info("[MobSync] Broadcaster started (interval=" + intervalTicks + " ticks).");
    }

    public void stop() {
        running = false;
        if (taskId >= 0) {
            Bukkit.getScheduler().cancelTask(taskId);
            taskId = -1;
        }
        LOGGER.info("[MobSync] Broadcaster stopped.");
    }

    // ── Broadcast tick (runs on main thread) ─────────────────────────────────

    private void tick() {
        RemoteDataServer server = RemoteDataServer.get();
        if (server == null || !server.hasSlaves()) return;

        for (org.bukkit.World world : Bukkit.getWorlds()) {
            for (Entity entity : world.getEntities()) {
                // Only sync actual mobs (not players, not armorstands, not projectiles, etc.)
                if (!(entity instanceof Mob mob)) continue;
                // Don't sync ghost mobs that slaves already spawned from a previous push
                if (mob.hasMetadata("slave_ghost_mob")) continue;

                broadcastMob(mob, world.getName());
            }
        }
    }

    private void broadcastMob(Mob mob, String worldName) {
        try {
            LivingEntity living = mob;
            float health    = (float) living.getHealth();
            float maxHealth = (float) living.getMaxHealth();
            Vector vel      = mob.getVelocity();

            byte flags = 0;
            if (health <= 0)          flags |= 0x01; // dead
            if (mob.isOnGround())     flags |= 0x02; // onGround

            String entityType = mob.getType().getKey().toString(); // e.g. "minecraft:zombie"

            MobSyncMessage msg = new MobSyncMessage(
                    MobSyncMessage.TYPE_FULL_STATE,
                    mob.getUniqueId(),
                    System.currentTimeMillis(),
                    entityType,
                    worldName,
                    mob.getLocation().getX(),
                    mob.getLocation().getY(),
                    mob.getLocation().getZ(),
                    mob.getLocation().getYaw(),
                    mob.getLocation().getPitch(),
                    vel.getX(), vel.getY(), vel.getZ(),
                    health, maxHealth, flags
            );

            // Fire-and-forget on virtual thread so we don't block main thread on I/O
            Thread.ofVirtual().name("MobSync-Broadcast-" + mob.getUniqueId()).start(() -> {
                RemoteDataServer srv = RemoteDataServer.get();
                if (srv != null) srv.broadcastMobPublic(msg);
            });

        } catch (Exception e) {
            LOGGER.log(Level.FINE, "[MobSync] broadcastMob error", e);
        }
    }

    // ── Called externally (e.g. after a health update) ───────────────────────

    /**
     * Immediately broadcast a health-update message for a single mob.
     * Call this from MobDamageListener after applying damage on master.
     */
    public void broadcastHealthUpdate(Mob mob, String worldName) {
        try {
            float health    = (float) mob.getHealth();
            float maxHealth = (float) mob.getMaxHealth();
            Vector vel      = mob.getVelocity();
            byte flags = 0;
            if (health <= 0)      flags |= 0x01;
            if (mob.isOnGround()) flags |= 0x02;

            MobSyncMessage msg = new MobSyncMessage(
                    MobSyncMessage.TYPE_HEALTH_UPDATE,
                    mob.getUniqueId(),
                    System.currentTimeMillis(),
                    mob.getType().getKey().toString(),
                    worldName,
                    mob.getLocation().getX(),
                    mob.getLocation().getY(),
                    mob.getLocation().getZ(),
                    mob.getLocation().getYaw(),
                    mob.getLocation().getPitch(),
                    vel.getX(), vel.getY(), vel.getZ(),
                    health, maxHealth, flags
            );

            Thread.ofVirtual().name("MobSync-HealthUpdate").start(() -> {
                RemoteDataServer srv = RemoteDataServer.get();
                if (srv != null) srv.broadcastMobPublic(msg);
            });

        } catch (Exception e) {
            LOGGER.log(Level.FINE, "[MobSync] broadcastHealthUpdate error", e);
        }
    }
}
