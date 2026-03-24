package org.purpurmc.purpur.network;

import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataManager – single entry point (v3, master-slave block sync).
 *
 * MASTER  → starts RemoteDataServer (TCP listener for slaves).
 *           isWorldOwner=true  → world files are read/write normally.
 *           RemoteBlockListener NOT registered (master applies events locally).
 *
 * SLAVE   → starts RemoteDataClient (connects to master).
 *           isWorldOwner=false → RemoteBlockListener is registered:
 *             • Cancels local block events (no writes to region files).
 *             • Forwards BLOCK_ACTION to master.
 *             • Renders BLOCK_PUSH from master.
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
                // Master: run the TCP server so slaves can connect
                RemoteDataServer server = RemoteDataServer.init(config, serverRoot);
                server.start();
                LOGGER.info("[RemoteData] Master server started on port " + config.masterPort);
                // Master also registers the listener so its own block changes get broadcast to slaves
                registerBlockListener(config);
            } else {
                // Slave: connect to master, register cancel+forward listener
                RemoteDataClient client = RemoteDataClient.init(config);
                client.start();
                LOGGER.info("[RemoteData] Slave client connecting to "
                        + config.masterHost + ":" + config.masterPort);
                registerBlockListener(config);
            }

            initialised = true;
            LOGGER.info("[RemoteData] ✓ Remote data system online.");

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[RemoteData] Failed to start remote data system – " +
                    "server will continue with LOCAL storage only.", e);
        }
    }

    private static void registerBlockListener(RemoteDataConfig config) {
        // Bukkit plugin registration requires a Plugin instance.
        // We hook into the first available plugin (typically the server itself via CraftServer).
        // In a Purpur/Paper server context this is called after plugins are loaded,
        // so we use the server's own internal plugin reference.
        try {
            Plugin plugin = Bukkit.getPluginManager().getPlugins().length > 0
                    ? Bukkit.getPluginManager().getPlugins()[0]
                    : null;
            if (plugin != null) {
                Bukkit.getPluginManager().registerEvents(new RemoteBlockListener(), plugin);
                LOGGER.info("[RemoteData] RemoteBlockListener registered (worldOwner=" + config.isWorldOwner + ")");
            } else {
                LOGGER.warning("[RemoteData] Could not register RemoteBlockListener – no plugins loaded yet.");
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[RemoteData] RemoteBlockListener registration failed", e);
        }
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
