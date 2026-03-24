package org.purpurmc.purpur.network;

import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RemoteDataConfig
 *
 * Reads from remote-data.yml in the server root directory.
 *
 * Example remote-data.yml:
 *
 * remote-data:
 *   enabled: true
 *   mode: client         # "master" or "client"
 *   master-host: "127.0.0.1"
 *   master-port: 25580
 *   secret-key: "change-me-to-something-secure"
 *   use-tls: false
 *   tls-keystore-path: "keystore.jks"
 *   tls-keystore-password: "changeit"
 *   cache:
 *     max-chunk-entries: 4096
 *     dirty-flush-interval-ticks: 100
 *     wal-enabled: true
 *     wal-path: "remote-wal.log"
 *   sync:
 *     batch-size: 32
 *     compression-enabled: true
 *     connect-timeout-ms: 5000
 *     read-timeout-ms: 3000
 */
public class RemoteDataConfig {

    private static final Logger LOGGER = Logger.getLogger("RemoteDataConfig");

    // ── singleton ────────────────────────────────────────────────────────────
    private static RemoteDataConfig INSTANCE;

    /**
     * FIX #6: Return null instead of throwing when not initialised yet.
     * The old code threw IllegalStateException which could crash the server
     * during early startup before the config was loaded.
     */
    public static RemoteDataConfig get() {
        return INSTANCE; // may be null – callers must null-check
    }

    public static RemoteDataConfig init(File serverRoot) {
        INSTANCE = new RemoteDataConfig(serverRoot);
        return INSTANCE;
    }

    // ── fields ───────────────────────────────────────────────────────────────
    public final boolean enabled;
    public final boolean isMaster;          // true = master mode, false = client mode
    public final String masterHost;
    public final int masterPort;
    public final String secretKey;

    // TLS
    public final boolean useTLS;
    public final String tlsKeystorePath;
    public final String tlsKeystorePassword;

    // Cache
    public final int maxChunkCacheEntries;
    public final int dirtyFlushIntervalTicks;
    public final boolean walEnabled;
    public final String walPath;

    // Sync
    public final int batchSize;
    public final boolean compressionEnabled;
    public final int connectTimeoutMs;
    public final int readTimeoutMs;

    // ── constructor ──────────────────────────────────────────────────────────
    private RemoteDataConfig(File serverRoot) {
        File configFile = new File(serverRoot, "remote-data.yml");

        YamlConfiguration cfg = new YamlConfiguration();

        if (configFile.exists()) {
            try {
                cfg.load(configFile);
            } catch (IOException | InvalidConfigurationException e) {
                LOGGER.log(Level.SEVERE, "[RemoteData] Failed to load remote-data.yml – using defaults", e);
            }
        } else {
            LOGGER.warning("[RemoteData] remote-data.yml not found – remote data system disabled.");
        }

        // ── root ──
        enabled = cfg.getBoolean("remote-data.enabled", false);
        String mode = cfg.getString("remote-data.mode", "client");
        isMaster = "master".equalsIgnoreCase(mode);
        masterHost = cfg.getString("remote-data.master-host", "127.0.0.1");
        masterPort = cfg.getInt("remote-data.master-port", 25580);
        secretKey = cfg.getString("remote-data.secret-key", "CHANGE_ME");

        // ── TLS ──
        useTLS = cfg.getBoolean("remote-data.use-tls", false);
        tlsKeystorePath = cfg.getString("remote-data.tls-keystore-path", "keystore.jks");
        tlsKeystorePassword = cfg.getString("remote-data.tls-keystore-password", "changeit");

        // ── Cache ──
        maxChunkCacheEntries = cfg.getInt("remote-data.cache.max-chunk-entries", 4096);
        dirtyFlushIntervalTicks = cfg.getInt("remote-data.cache.dirty-flush-interval-ticks", 100);
        walEnabled = cfg.getBoolean("remote-data.cache.wal-enabled", true);
        walPath = cfg.getString("remote-data.cache.wal-path", "remote-wal.log");

        // ── Sync ──
        batchSize = cfg.getInt("remote-data.sync.batch-size", 32);
        compressionEnabled = cfg.getBoolean("remote-data.sync.compression-enabled", true);
        connectTimeoutMs = cfg.getInt("remote-data.sync.connect-timeout-ms", 5000);
        readTimeoutMs = cfg.getInt("remote-data.sync.read-timeout-ms", 3000);

        // Save defaults back if file didn't exist
        if (!configFile.exists()) {
            writeDefaults(configFile, cfg);
        }

        if (enabled) {
            LOGGER.info("[RemoteData] Mode: " + (isMaster ? "MASTER" : "CLIENT")
                    + " | Host: " + masterHost + ":" + masterPort
                    + " | TLS: " + useTLS
                    + " | Compression: " + compressionEnabled);
        }
    }

    private void writeDefaults(File configFile, YamlConfiguration cfg) {
        cfg.set("remote-data.enabled", false);
        cfg.set("remote-data.mode", "client");
        cfg.set("remote-data.master-host", "127.0.0.1");
        cfg.set("remote-data.master-port", 25580);
        cfg.set("remote-data.secret-key", "CHANGE_ME");
        cfg.set("remote-data.use-tls", false);
        cfg.set("remote-data.tls-keystore-path", "keystore.jks");
        cfg.set("remote-data.tls-keystore-password", "changeit");
        cfg.set("remote-data.cache.max-chunk-entries", 4096);
        cfg.set("remote-data.cache.dirty-flush-interval-ticks", 100);
        cfg.set("remote-data.cache.wal-enabled", true);
        cfg.set("remote-data.cache.wal-path", "remote-wal.log");
        cfg.set("remote-data.sync.batch-size", 32);
        cfg.set("remote-data.sync.compression-enabled", true);
        cfg.set("remote-data.sync.connect-timeout-ms", 5000);
        cfg.set("remote-data.sync.read-timeout-ms", 3000);
        try {
            cfg.save(configFile);
            LOGGER.info("[RemoteData] Generated default remote-data.yml");
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "[RemoteData] Could not save default remote-data.yml", e);
        }
    }
}
