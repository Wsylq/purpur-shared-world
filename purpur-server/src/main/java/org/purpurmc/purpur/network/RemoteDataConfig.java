package org.purpurmc.purpur.network;

import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataConfig — UNCHANGED
 *
 * Two separate timeouts:
 *   shortTimeoutMs    – PING / GET / DELETE / AUTH  (default 5 000 ms)
 *   chunkPutTimeoutMs – PUT of chunk/* keys         (default 30 000 ms)
 */
public class RemoteDataConfig {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataConfig");

    private static RemoteDataConfig INSTANCE;
    public static RemoteDataConfig get()                    { return INSTANCE; }
    public static RemoteDataConfig init(File serverRoot)    { INSTANCE = new RemoteDataConfig(serverRoot); return INSTANCE; }

    public final boolean enabled;
    public final boolean isMaster;
    public final String  masterHost;
    public final int     masterPort;
    public final String  secretKey;

    public final boolean useTLS;
    public final String  tlsKeystorePath;
    public final String  tlsKeystorePassword;

    public final int     maxChunkCacheEntries;
    public final int     dirtyFlushIntervalTicks;
    public final boolean walEnabled;
    public final String  walPath;

    public final int     batchSize;
    public final boolean compressionEnabled;
    public final int     connectTimeoutMs;
    public final long    shortTimeoutMs;
    public final long    chunkPutTimeoutMs;

    private RemoteDataConfig(File serverRoot) {
        File configFile = new File(serverRoot, "remote-data.yml");
        YamlConfiguration cfg = new YamlConfiguration();
        if (configFile.exists()) {
            try { cfg.load(configFile); }
            catch (IOException | InvalidConfigurationException e) {
                LOGGER.log(Level.SEVERE, "[RemoteData] Failed to load remote-data.yml – using defaults", e);
            }
        } else {
            LOGGER.warning("[RemoteData] remote-data.yml not found – remote data system disabled.");
        }

        enabled    = cfg.getBoolean("remote-data.enabled", false);
        String mode = cfg.getString("remote-data.mode", "client");
        isMaster   = "master".equalsIgnoreCase(mode);
        masterHost = cfg.getString("remote-data.master-host", "127.0.0.1");
        masterPort = cfg.getInt   ("remote-data.master-port", 25580);
        secretKey  = cfg.getString("remote-data.secret-key",  "CHANGE_ME");

        useTLS              = cfg.getBoolean("remote-data.use-tls", false);
        tlsKeystorePath     = cfg.getString ("remote-data.tls-keystore-path",     "keystore.jks");
        tlsKeystorePassword = cfg.getString ("remote-data.tls-keystore-password", "changeit");

        maxChunkCacheEntries    = cfg.getInt    ("remote-data.cache.max-chunk-entries",          4096);
        dirtyFlushIntervalTicks = cfg.getInt    ("remote-data.cache.dirty-flush-interval-ticks", 100);
        walEnabled              = cfg.getBoolean("remote-data.cache.wal-enabled", true);
        walPath                 = cfg.getString ("remote-data.cache.wal-path",    "remote-wal.log");

        batchSize          = cfg.getInt    ("remote-data.sync.batch-size",          32);
        compressionEnabled = cfg.getBoolean("remote-data.sync.compression-enabled", true);
        connectTimeoutMs   = cfg.getInt    ("remote-data.sync.connect-timeout-ms",  5000);

        int legacyTimeout  = cfg.getInt("remote-data.sync.read-timeout-ms", -1);
        shortTimeoutMs     = cfg.getLong("remote-data.sync.short-timeout-ms",
                legacyTimeout > 0 ? legacyTimeout : 5000L);
        chunkPutTimeoutMs  = cfg.getLong("remote-data.sync.chunk-put-timeout-ms", 30_000L);

        if (!configFile.exists()) writeDefaults(configFile, cfg);

        if (enabled) {
            LOGGER.info("[RemoteData] Mode: " + (isMaster ? "MASTER" : "CLIENT")
                    + " | Host: " + masterHost + ":" + masterPort
                    + " | TLS: " + useTLS
                    + " | Compression: " + compressionEnabled
                    + " | shortTimeout: " + shortTimeoutMs + "ms"
                    + " | chunkPutTimeout: " + chunkPutTimeoutMs + "ms");
        }
    }

    private void writeDefaults(File configFile, YamlConfiguration cfg) {
        cfg.set("remote-data.enabled",                               false);
        cfg.set("remote-data.mode",                                  "client");
        cfg.set("remote-data.master-host",                           "127.0.0.1");
        cfg.set("remote-data.master-port",                           25580);
        cfg.set("remote-data.secret-key",                            "CHANGE_ME");
        cfg.set("remote-data.use-tls",                               false);
        cfg.set("remote-data.tls-keystore-path",                     "keystore.jks");
        cfg.set("remote-data.tls-keystore-password",                 "changeit");
        cfg.set("remote-data.cache.max-chunk-entries",               4096);
        cfg.set("remote-data.cache.dirty-flush-interval-ticks",      100);
        cfg.set("remote-data.cache.wal-enabled",                     true);
        cfg.set("remote-data.cache.wal-path",                        "remote-wal.log");
        cfg.set("remote-data.sync.batch-size",                       32);
        cfg.set("remote-data.sync.compression-enabled",              true);
        cfg.set("remote-data.sync.connect-timeout-ms",               5000);
        cfg.set("remote-data.sync.short-timeout-ms",                 5000);
        cfg.set("remote-data.sync.chunk-put-timeout-ms",             30000);
        try { cfg.save(configFile); LOGGER.info("[RemoteData] Generated default remote-data.yml"); }
        catch (IOException e) { LOGGER.log(Level.WARNING, "[RemoteData] Could not write default config", e); }
    }
}
