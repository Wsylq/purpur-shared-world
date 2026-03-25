package org.purpurmc.purpur.network;

import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataManager – single entry point (v4, master-slave block sync, fixed).
 *
 * MASTER  → starts RemoteDataServer (TCP listener for slaves).
 *           isWorldOwner=true  → world files read/write normally.
 *           RemoteBlockListener registered at MONITOR to broadcast to slaves.
 *
 * SLAVE   → starts RemoteDataClient (connects to master).
 *           isWorldOwner=false → RemoteBlockListener registered at LOWEST:
 *             • Cancels local block events (no writes to region files).
 *             • Forwards BLOCK_ACTION to master.
 *             • Applies BLOCK_PUSH from master.
 *
 * Listener registration fix (v4):
 *   Wait for Bukkit to finish plugin loading before registering, using a
 *   scheduler task on the very first tick. This avoids the race condition where
 *   RemoteDataManager.init() is called from MinecraftServer (before any plugin
 *   is loaded) and getPlugins() returns an empty array.
 */
public class RemoteDataManager {

    private static final Logger  LOGGER      = Logger.getLogger("RemoteDataManager");
    private static volatile boolean initialised = false;

    public static synchronized void init(File serverRoot) {
        if (initialised) return;

        RemoteDataConfig config = RemoteDataConfig.init(serverRoot);

        if (!config.enabled) {
            LOGGER.info("[RemoteData] System is DISABLED (remote-data.enabled=false). Skipping init.");
            return;
        }

        LOGGER.info("[RemoteData] Initialising remote data system (mode="
                + (config.isMaster ? "MASTER" : "SLAVE")
                + ", worldOwner=" + config.isWorldOwner + ")...");

        try {
            RemoteDataCache cache = RemoteDataCache.init(config, serverRoot);
            cache.start();

            if (config.isMaster) {
                RemoteDataServer server = RemoteDataServer.init(config, serverRoot);
                server.start();
                LOGGER.info("[RemoteData] Master server started on port " + config.masterPort);
            } else {
                RemoteDataClient client = RemoteDataClient.init(config);
                client.start();
                LOGGER.info("[RemoteData] Slave client connecting to "
                        + config.masterHost + ":" + config.masterPort);
            }

            // Defer listener registration until after all plugins are loaded (first tick)
            scheduleListenerRegistration(config);

            initialised = true;
            LOGGER.info("[RemoteData] ✓ Remote data system online.");

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[RemoteData] Failed to start remote data system – "
                    + "server will continue with LOCAL storage only.", e);
        }
    }

    /**
     * Schedule RemoteBlockListener registration for tick 1 (after all plugins load).
     * Falls back to immediate registration if the scheduler isn't available yet
     * (e.g., called post-startup).
     */
    private static void scheduleListenerRegistration(RemoteDataConfig config) {
        // Try immediate registration first (works if called post-plugin-load)
        Plugin plugin = findPlugin();
        if (plugin != null) {
            registerBlockListener(plugin, config);
            return;
        }

        // Plugins not loaded yet – schedule for next tick via a daemon thread poll
        Thread waiter = new Thread(() -> {
            for (int attempts = 0; attempts < 120; attempts++) { // wait up to 60s
                try { Thread.sleep(500); } catch (InterruptedException e) { return; }
                Plugin p = findPlugin();
                if (p != null) {
                    // Must register on main thread via scheduler
                    Bukkit.getScheduler().runTask(p, () -> registerBlockListener(p, config));
                    return;
                }
            }
            LOGGER.warning("[RemoteData] Could not register RemoteBlockListener – no plugin found after 60s");
        }, "RemoteData-ListenerRegistrar");
        waiter.setDaemon(true);
        waiter.start();
    }

    private static void registerBlockListener(Plugin plugin, RemoteDataConfig config) {
        try {
            Bukkit.getPluginManager().registerEvents(new RemoteBlockListener(), plugin);
            LOGGER.info("[RemoteData] RemoteBlockListener registered via plugin '"
                    + plugin.getName() + "' (worldOwner=" + config.isWorldOwner + ")");
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] RemoteBlockListener registration failed", e);
        }
    }

    /** Returns the first enabled plugin found, or null if none loaded yet. */
    private static Plugin findPlugin() {
        try {
            Plugin[] plugins = Bukkit.getPluginManager().getPlugins();
            for (Plugin p : plugins) {
                if (p.isEnabled()) return p;
            }
        } catch (Exception ignored) {}
        return null;
    }

    public static synchronized void shutdown() {
        if (!initialised) return;
        LOGGER.info("[RemoteData] Shutting down remote data system...");

        RemoteDataCache cache = RemoteDataCache.get();
        if (cache != null) cache.stop();

        RemoteDataClient client = RemoteDataClient.get();
        if (client != null) client.stop();

        RemoteDataServer server = RemoteDataServer.get();
        if (server != null) server.stop();

        initialised = false;
        LOGGER.info("[RemoteData] ✓ Remote data system shut down cleanly.");
    }

    public static boolean isInitialised() { return initialised; }
}
