package org.purpurmc.purpur.network;

import io.papermc.paper.event.player.AsyncChatEvent;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.bukkit.Bukkit;
import org.bukkit.advancement.Advancement;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.player.PlayerAdvancementDoneEvent;
import org.bukkit.event.player.PlayerCommandPreprocessEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

import java.util.Set;
import java.util.logging.Logger;

/**
 * RemoteChatListener – cross-server chat, advancement, join/quit, death, and command sync.
 *
 * ── SLAVE mode ────────────────────────────────────────────────────────────────
 *   • Intercepts all player-visible events at HIGHEST priority.
 *   • Sends a ChatSyncMessage to master via RemoteDataClient.sendChatEvent().
 *   • Suppresses the local broadcast so players on this server don't see a
 *     duplicate (the master will echo it back to ALL servers including this one).
 *
 * ── MASTER mode ───────────────────────────────────────────────────────────────
 *   • MONITOR priority: after the event is processed locally, broadcast
 *     ChatSyncMessage to all connected slaves via RemoteDataServer.broadcastChatPublic().
 *   • The master does NOT suppress its own local broadcast — players on the
 *     master still see the message normally through Bukkit.
 *
 * ── Receive path (both modes, called from RemoteDataClient.handleServerPush) ──
 *   • RemoteDataClient decodes a CHAT_PUSH packet and calls
 *     RemoteChatListener.applyPush(ChatSyncMessage) on the main thread.
 *   • applyPush() broadcasts the formatted message to all players on THIS server.
 *
 * ── Commands synced ───────────────────────────────────────────────────────────
 *   Only social commands are relayed: /me, /say, /teammsg, /tm.
 *   Regular game commands (/tp, /give, etc.) are NOT relayed — they only run
 *   on the server where the player is physically present.
 */
public class RemoteChatListener implements Listener {

    private static final Logger LOGGER = Logger.getLogger("RemoteChatListener");

    /** Commands whose output should be broadcast cross-server. */
    private static final Set<String> SOCIAL_COMMANDS = Set.of(
            "/me", "/say", "/teammsg", "/tm"
    );

    // ── Static tag to suppress re-broadcast of incoming remote messages ───────
    // When applyPush() calls Bukkit.broadcast(), it sets this flag so the
    // MONITOR handlers on THIS server don't re-forward the message.
    private static final ThreadLocal<Boolean> IS_REMOTE_BROADCAST = ThreadLocal.withInitial(() -> false);

    // ════════════════════════════════════════════════════════════════════════
    // SLAVE – HIGHEST priority: intercept → send to master → cancel local
    // ════════════════════════════════════════════════════════════════════════

    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = false)
    public void onChat_slave(AsyncChatEvent event) {
        if (!isSlaveMode()) return;

        String playerName = event.getPlayer().getName();
        String message    = PlainTextComponentSerializer.plainText().serialize(event.message());

        // Cancel the local Bukkit broadcast — master will echo it back to everyone
        event.setCancelled(true);

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_CHAT,
                System.currentTimeMillis(),
                playerName,
                message,
                getServerName());

        sendToMaster(msg);
    }

    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true)
    public void onJoin_slave(PlayerJoinEvent event) {
        if (!isSlaveMode()) return;

        // Suppress the default join message on the slave;
        // master will broadcast to all servers.
        event.joinMessage(null);

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_JOIN,
                System.currentTimeMillis(),
                event.getPlayer().getName(),
                "",
                getServerName());

        sendToMaster(msg);
    }

    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true)
    public void onQuit_slave(PlayerQuitEvent event) {
        if (!isSlaveMode()) return;

        event.quitMessage(null);

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_QUIT,
                System.currentTimeMillis(),
                event.getPlayer().getName(),
                "",
                getServerName());

        sendToMaster(msg);
    }

    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true)
    public void onDeath_slave(PlayerDeathEvent event) {
        if (!isSlaveMode()) return;

        Component deathMsg = event.deathMessage();
        String    rawDeath = deathMsg != null
                ? PlainTextComponentSerializer.plainText().serialize(deathMsg)
                : event.getEntity().getName() + " died";

        // Suppress local death message broadcast
        event.deathMessage(null);

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_DEATH,
                System.currentTimeMillis(),
                event.getEntity().getName(),
                rawDeath,
                getServerName());

        sendToMaster(msg);
    }

    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = true)
    public void onAdvancement_slave(PlayerAdvancementDoneEvent event) {
        if (!isSlaveMode()) return;

        Advancement adv = event.getAdvancement();

        // Only relay advancements that have a display (i.e. ones shown in chat)
        if (adv.getDisplay() == null) return;
        String title = adv.getDisplay().title() != null
                ? PlainTextComponentSerializer.plainText().serialize(adv.getDisplay().title())
                : adv.getKey().getKey();

        // Paper suppresses the chat message when message() returns null — but
        // PlayerAdvancementDoneEvent doesn't let us null the component easily here.
        // We suppress by cancelling the message via the event if supported,
        // otherwise we rely on the master echo arriving within the same tick.
        // Best-effort: set the message to empty component.
        try {
            event.message(Component.empty());
        } catch (Exception ignored) {}

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_ADVANCEMENT,
                System.currentTimeMillis(),
                event.getPlayer().getName(),
                title,
                getServerName());

        sendToMaster(msg);
    }

    @EventHandler(priority = EventPriority.HIGHEST, ignoreCancelled = false)
    public void onCommand_slave(PlayerCommandPreprocessEvent event) {
        if (!isSlaveMode()) return;

        String cmd = event.getMessage().toLowerCase();
        boolean isSocial = SOCIAL_COMMANDS.stream().anyMatch(s -> cmd.equals(s) || cmd.startsWith(s + " "));
        if (!isSocial) return;

        // Let the command run locally on the slave too (so /me still works here),
        // but also forward it so master can relay the output to other slaves.
        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_COMMAND,
                System.currentTimeMillis(),
                event.getPlayer().getName(),
                event.getMessage(),
                getServerName());

        sendToMaster(msg);
        // Don't cancel — let it execute locally as well
    }

    // ════════════════════════════════════════════════════════════════════════
    // MASTER – MONITOR priority: broadcast to slaves after local processing
    // ════════════════════════════════════════════════════════════════════════

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onChat_master(AsyncChatEvent event) {
        if (!isMasterMode()) return;
        if (IS_REMOTE_BROADCAST.get()) return; // this was an applyPush echo, skip

        String playerName = event.getPlayer().getName();
        String message    = PlainTextComponentSerializer.plainText().serialize(event.message());

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_CHAT,
                System.currentTimeMillis(),
                playerName,
                message,
                getServerName());

        broadcastToSlaves(msg);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onJoin_master(PlayerJoinEvent event) {
        if (!isMasterMode()) return;
        if (IS_REMOTE_BROADCAST.get()) return;

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_JOIN,
                System.currentTimeMillis(),
                event.getPlayer().getName(),
                "",
                getServerName());

        broadcastToSlaves(msg);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onQuit_master(PlayerQuitEvent event) {
        if (!isMasterMode()) return;
        if (IS_REMOTE_BROADCAST.get()) return;

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_QUIT,
                System.currentTimeMillis(),
                event.getPlayer().getName(),
                "",
                getServerName());

        broadcastToSlaves(msg);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onDeath_master(PlayerDeathEvent event) {
        if (!isMasterMode()) return;
        if (IS_REMOTE_BROADCAST.get()) return;

        Component deathMsg = event.deathMessage();
        String rawDeath = deathMsg != null
                ? PlainTextComponentSerializer.plainText().serialize(deathMsg)
                : event.getEntity().getName() + " died";

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_DEATH,
                System.currentTimeMillis(),
                event.getEntity().getName(),
                rawDeath,
                getServerName());

        broadcastToSlaves(msg);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onAdvancement_master(PlayerAdvancementDoneEvent event) {
        if (!isMasterMode()) return;
        if (IS_REMOTE_BROADCAST.get()) return;
        if (event.getAdvancement().getDisplay() == null) return;

        String title = event.getAdvancement().getDisplay().title() != null
                ? PlainTextComponentSerializer.plainText().serialize(event.getAdvancement().getDisplay().title())
                : event.getAdvancement().getKey().getKey();

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_ADVANCEMENT,
                System.currentTimeMillis(),
                event.getPlayer().getName(),
                title,
                getServerName());

        broadcastToSlaves(msg);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onCommand_master(PlayerCommandPreprocessEvent event) {
        if (!isMasterMode()) return;
        if (IS_REMOTE_BROADCAST.get()) return;

        String cmd = event.getMessage().toLowerCase();
        boolean isSocial = SOCIAL_COMMANDS.stream().anyMatch(s -> cmd.equals(s) || cmd.startsWith(s + " "));
        if (!isSocial) return;

        ChatSyncMessage msg = new ChatSyncMessage(
                ChatSyncMessage.TYPE_COMMAND,
                System.currentTimeMillis(),
                event.getPlayer().getName(),
                event.getMessage(),
                getServerName());

        broadcastToSlaves(msg);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Apply path — called from RemoteDataClient / RemoteDataServer on receive
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Apply an incoming ChatSyncMessage received from another server.
     * Must be called on the main thread (or async-safe for broadcast).
     * Master calls this for SLAVE events before re-broadcasting.
     * Slaves call this for CHAT_PUSH messages from master.
     */
    public static void applyPush(ChatSyncMessage msg) {
        IS_REMOTE_BROADCAST.set(true);
        try {
            Component formatted = buildComponent(msg);
            if (formatted != null) {
                Bukkit.broadcast(formatted);
            }
        } finally {
            IS_REMOTE_BROADCAST.set(false);
        }
        LOGGER.fine("[RemoteChat] Applied push: " + msg);
    }

    // ════════════════════════════════════════════════════════════════════════
    // Formatting
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Build the Component that will be broadcast to all online players.
     * Uses Adventure's mini-message style via Component.text() so it renders
     * correctly in all chat implementations (vanilla, custom, etc.).
     */
    private static Component buildComponent(ChatSyncMessage msg) {
        String server = "[" + msg.serverName + "] ";
        return switch (msg.type) {
            case ChatSyncMessage.TYPE_CHAT ->
                net.kyori.adventure.text.Component.text()
                    .append(net.kyori.adventure.text.Component.text(server)
                        .color(net.kyori.adventure.text.format.NamedTextColor.GRAY))
                    .append(net.kyori.adventure.text.Component.text("<" + msg.playerName + "> ")
                        .color(net.kyori.adventure.text.format.NamedTextColor.WHITE))
                    .append(net.kyori.adventure.text.Component.text(msg.message)
                        .color(net.kyori.adventure.text.format.NamedTextColor.WHITE))
                    .build();

            case ChatSyncMessage.TYPE_JOIN ->
                net.kyori.adventure.text.Component.text()
                    .append(net.kyori.adventure.text.Component.text(server)
                        .color(net.kyori.adventure.text.format.NamedTextColor.GRAY))
                    .append(net.kyori.adventure.text.Component.text(msg.playerName + " joined the game")
                        .color(net.kyori.adventure.text.format.NamedTextColor.YELLOW))
                    .build();

            case ChatSyncMessage.TYPE_QUIT ->
                net.kyori.adventure.text.Component.text()
                    .append(net.kyori.adventure.text.Component.text(server)
                        .color(net.kyori.adventure.text.format.NamedTextColor.GRAY))
                    .append(net.kyori.adventure.text.Component.text(msg.playerName + " left the game")
                        .color(net.kyori.adventure.text.format.NamedTextColor.YELLOW))
                    .build();

            case ChatSyncMessage.TYPE_DEATH ->
                net.kyori.adventure.text.Component.text()
                    .append(net.kyori.adventure.text.Component.text(server)
                        .color(net.kyori.adventure.text.format.NamedTextColor.GRAY))
                    .append(net.kyori.adventure.text.Component.text(msg.message)
                        .color(net.kyori.adventure.text.format.NamedTextColor.RED))
                    .build();

            case ChatSyncMessage.TYPE_ADVANCEMENT ->
                net.kyori.adventure.text.Component.text()
                    .append(net.kyori.adventure.text.Component.text(server)
                        .color(net.kyori.adventure.text.format.NamedTextColor.GRAY))
                    .append(net.kyori.adventure.text.Component.text(msg.playerName)
                        .color(net.kyori.adventure.text.format.NamedTextColor.GREEN))
                    .append(net.kyori.adventure.text.Component.text(" has made the advancement ")
                        .color(net.kyori.adventure.text.format.NamedTextColor.WHITE))
                    .append(net.kyori.adventure.text.Component.text("[" + msg.message + "]")
                        .color(net.kyori.adventure.text.format.NamedTextColor.GREEN))
                    .build();

            case ChatSyncMessage.TYPE_COMMAND ->
                // /me and /say output — the message already has the formatted text
                net.kyori.adventure.text.Component.text()
                    .append(net.kyori.adventure.text.Component.text(server)
                        .color(net.kyori.adventure.text.format.NamedTextColor.GRAY))
                    .append(net.kyori.adventure.text.Component.text(msg.message)
                        .color(net.kyori.adventure.text.format.NamedTextColor.WHITE))
                    .build();

            default -> null;
        };
    }

    // ════════════════════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════════════════════

    private static void sendToMaster(ChatSyncMessage msg) {
        RemoteDataClient client = RemoteDataClient.get();
        if (client == null || !client.isConnected()) {
            LOGGER.fine("[RemoteChat] sendToMaster skipped – not connected");
            return;
        }
        // Inline the send: encode here, delegate to the client's sendChatEvent.
        // We call it directly on a virtual thread to stay off the main thread.
        final RemoteDataClient c = client;
        Thread.ofVirtual().name("RemoteChat-Send").start(() -> {
            try {
                c.sendChatEventDirect(msg);
            } catch (Exception e) {
                LOGGER.fine("[RemoteChat] sendToMaster failed: " + e.getMessage());
            }
        });
    }

    private static void broadcastToSlaves(ChatSyncMessage msg) {
        RemoteDataServer server = RemoteDataServer.get();
        if (server == null) return;
        server.broadcastChatPublic(msg);
    }

    private static boolean isSlaveMode() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        return cfg != null && cfg.enabled && !cfg.isWorldOwner;
    }

    private static boolean isMasterMode() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        return cfg != null && cfg.enabled && cfg.isWorldOwner && cfg.isMaster;
    }

    /**
     * The server name shown in chat brackets, e.g. "[Play-1]".
     * Reads from remote-data.yml: remote-data.server-name
     * Falls back to the Bukkit server name / "Server".
     */
    private static String getServerName() {
        RemoteDataConfig cfg = RemoteDataConfig.get();
        if (cfg != null && cfg.serverName != null && !cfg.serverName.isBlank()) {
            return cfg.serverName;
        }
        String bukkit = Bukkit.getServer().getName();
        return (bukkit != null && !bukkit.isBlank()) ? bukkit : "Server";
    }
}
