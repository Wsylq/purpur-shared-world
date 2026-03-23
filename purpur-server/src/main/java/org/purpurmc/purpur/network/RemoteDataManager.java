package org.purpurmc.purpur.network;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataManager – single entry point for the entire remote data system.
 *
 * Call RemoteDataManager.init(serverRoot) from the server startup sequence.
 * The best place is inside MinecraftServer.runServer() BEFORE world loading,
 * or inside CraftServer constructor after PurpurConfig is loaded.
 *
 * Patch location suggestion:
 *   patches/server/0xxx-Remote-Data-System.patch
 *   Target class: net.minecraft.server.MinecraftServer
 *   Target method: protected void runServer()
 *   Insert BEFORE: this.loadWorld() or equivalent
 *
 * Example patch snippet (pseudo-diff):
 * +    // Purpur – Remote Data System
 * +    org.purpurmc.purpur.network.RemoteDataManager.init(this.getServerDirectory());
 *
 * On shutdown insert BEFORE server close:
 * +    org.purpurmc.purpur.network.RemoteDataManager.shutdown();
 */
public class RemoteDataManager {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataManager");
    private static volatile boolean initialised = false;

    /**
     * Initialise the full remote data stack.
     * Safe to call multiple times – subsequent calls are no-ops.
     *
     * @param serverRoot  the server's working directory (where server.properties lives)
     */
    public static synchronized void init(File serverRoot) {
        if (initialised) return;

        // 1. Load config from remote-data.yml
        RemoteDataConfig config = RemoteDataConfig.init(serverRoot);

        if (!config.enabled) {
            LOGGER.info("[RemoteData] System is DISABLED (remote-data.enabled=false). Skipping init.");
            return;
        }

        LOGGER.info("[RemoteData] Initialising remote data system (mode="
                + (config.isMaster ? "MASTER" : "CLIENT") + ")...");

        try {
            // 2. Start cache layer (both master and client need it)
            RemoteDataCache cache = RemoteDataCache.init(config, serverRoot);
            cache.start();

            if (config.isMaster) {
                // 3a. Start the master TCP server
                RemoteDataServer server = RemoteDataServer.init(config, serverRoot);
                server.start();
                LOGGER.info("[RemoteData] Master server started on port " + config.masterPort);
            } else {
                // 3b. Start the client that connects to the master
                RemoteDataClient client = RemoteDataClient.init(config);
                client.start();
                LOGGER.info("[RemoteData] Client connecting to "
                        + config.masterHost + ":" + config.masterPort);
            }

            initialised = true;
            LOGGER.info("[RemoteData] ✓ Remote data system online.");

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[RemoteData] Failed to start remote data system – " +
                    "server will continue with LOCAL storage only.", e);
        }
    }

    /**
     * Gracefully shut down the remote data system.
     * Call this from the server shutdown sequence BEFORE worlds are closed
     * so all dirty data gets flushed.
     */
    public static synchronized void shutdown() {
        if (!initialised) return;
        LOGGER.info("[RemoteData] Shutting down remote data system...");

        // Stop cache first (triggers final flush)
        RemoteDataCache cache = RemoteDataCache.get();
        if (cache != null) cache.stop();

        // Stop client/server
        RemoteDataClient client = RemoteDataClient.get();
        if (client != null) client.stop();

        RemoteDataServer server = RemoteDataServer.get();
        if (server != null) server.stop();

        initialised = false;
        LOGGER.info("[RemoteData] ✓ Remote data system shut down cleanly.");
    }

    public static boolean isInitialised() { return initialised; }
}
