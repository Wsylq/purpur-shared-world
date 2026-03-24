package org.purpurmc.purpur.network;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataManager – single entry point for the entire remote data system. — UNCHANGED
 *
 * Call RemoteDataManager.init(serverRoot) from MinecraftServer.runServer()
 * BEFORE world loading. Call shutdown() from the server shutdown sequence
 * BEFORE worlds are closed so dirty data is flushed.
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
                + (config.isMaster ? "MASTER" : "CLIENT") + ")...");

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
