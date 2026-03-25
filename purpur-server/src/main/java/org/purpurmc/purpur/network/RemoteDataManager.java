package org.purpurmc.purpur.network;

import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataManager – single entry point (v6, block + chat + mob sync).
 *
 * v6 changes:
 *   MASTER: starts MobSyncBroadcaster, registers MobDamageListener.
 *   SLAVE:  registers MobSpawnListener (blocks natural spawning).
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
                // Init broadcaster (started later when a plugin is available)
                MobSyncBroadcaster.init(config);
                LOGGER.info("[RemoteData] Master server started on port " + config.masterPort);
            } else {
                RemoteDataClient client = RemoteDataClient.init(config);
                client.start();
                LOGGER.info("[RemoteData] Slave client connecting to "
                        + config.masterHost + ":" + config.masterPort);
            }

            scheduleListenerRegistration(config);

            initialised = true;
            LOGGER.info("[RemoteData] ✓ Remote data system online.");

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[RemoteData] Failed to start remote data system – "
                    + "server will continue with LOCAL storage only.", e);
        }
    }

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
            LOGGER.info("[RemoteData] RemoteBlockListener registered (worldOwner=" + config.isWorldOwner + ")");

            // Chat sync listener (chat/advancement/join/quit/death/commands)
            Bukkit.getPluginManager().registerEvents(new RemoteChatListener(), plugin);
            LOGGER.info("[RemoteData] RemoteChatListener registered (serverName=" + config.serverName + ")");

            if (config.isMaster) {
                // Damage listener: broadcasts health updates to slaves immediately on hit
                Bukkit.getPluginManager().registerEvents(new MobDamageListener(), plugin);
                LOGGER.info("[RemoteData] MobDamageListener registered (MASTER)");

                // Start the periodic mob position broadcaster
                MobSyncBroadcaster broadcaster = MobSyncBroadcaster.get();
                if (broadcaster != null) {
                    broadcaster.start(plugin);
                    LOGGER.info("[RemoteData] MobSyncBroadcaster started (MASTER)");
                }
            } else {
                // Spawn blocker: prevent natural mob spawning on slaves
                Bukkit.getPluginManager().registerEvents(new MobSpawnListener(), plugin);
                LOGGER.info("[RemoteData] MobSpawnListener registered (SLAVE – natural spawning blocked)");
            }

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

        MobSyncBroadcaster broadcaster = MobSyncBroadcaster.get();
        if (broadcaster != null) broadcaster.stop();

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
