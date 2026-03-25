package org.purpurmc.purpur.network;

import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataManager – single entry point (v5, block + chat sync).
 *
 * v5 change: registers RemoteChatListener alongside RemoteBlockListener.
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
                + ", worldOwner=" + config.isWorldOwner
                + ", serverName=" + config.serverName + ")...");

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
     * Schedule both RemoteBlockListener and RemoteChatListener registration for tick 1.
     * Falls back to immediate registration if plugins are already loaded.
     */
    private static void scheduleListenerRegistration(RemoteDataConfig config) {
        Plugin plugin = findPlugin();
        if (plugin != null) {
            registerListeners(plugin, config);
            return;
        }

        Thread waiter = new Thread(() -> {
            for (int attempts = 0; attempts < 120; attempts++) {
                try { Thread.sleep(500); } catch (InterruptedException e) { return; }
                Plugin p = findPlugin();
                if (p != null) {
                    Bukkit.getScheduler().runTask(p, () -> registerListeners(p, config));
                    return;
                }
            }
            LOGGER.warning("[RemoteData] Could not register listeners – no plugin found after 60s");
        }, "RemoteData-ListenerRegistrar");
        waiter.setDaemon(true);
        waiter.start();
    }

    private static void registerListeners(Plugin plugin, RemoteDataConfig config) {
        try {
            // Block sync listener (place/break/piston/etc.)
            Bukkit.getPluginManager().registerEvents(new RemoteBlockListener(), plugin);
            LOGGER.info("[RemoteData] RemoteBlockListener registered via plugin '"
                    + plugin.getName() + "' (worldOwner=" + config.isWorldOwner + ")");

            // Chat sync listener (chat/advancement/join/quit/death/commands)
            Bukkit.getPluginManager().registerEvents(new RemoteChatListener(), plugin);
            LOGGER.info("[RemoteData] RemoteChatListener registered via plugin '"
                    + plugin.getName() + "' (serverName=" + config.serverName + ")");

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Listener registration failed", e);
        }
    }

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

        RemoteDataCache  cache  = RemoteDataCache.get();
        if (cache  != null) cache.stop();

        RemoteDataClient client = RemoteDataClient.get();
        if (client != null) client.stop();

        RemoteDataServer server = RemoteDataServer.get();
        if (server != null) server.stop();

        initialised = false;
        LOGGER.info("[RemoteData] ✓ Remote data system shut down cleanly.");
    }

    public static boolean isInitialised() { return initialised; }
}
