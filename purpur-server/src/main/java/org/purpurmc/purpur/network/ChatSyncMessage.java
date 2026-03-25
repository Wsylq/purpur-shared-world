package org.purpurmc.purpur.network;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Lightweight binary message for cross-server chat, advancement, join/leave, and death events.
 *
 * Wire format (big-endian):
 *   [1 byte ] type         See TYPE_* constants
 *   [8 bytes] timestamp    System.currentTimeMillis()
 *   [2 bytes] playerNameLength
 *   [N bytes] playerName   UTF-8
 *   [2 bytes] messageLength
 *   [M bytes] message      UTF-8 – the chat text, advancement title, death message, command, etc.
 *   [2 bytes] serverNameLength
 *   [K bytes] serverName   UTF-8 – display name of the originating server (e.g. "Play-1")
 *
 * Total ≈ 30–300 bytes per event.
 */
public final class ChatSyncMessage {

    // ── Event types ────────────────────────────────────────────────────────────
    /** A player sent a chat message. */
    public static final byte TYPE_CHAT        = 0x01;
    /** A player completed an advancement. */
    public static final byte TYPE_ADVANCEMENT = 0x02;
    /** A player joined. */
    public static final byte TYPE_JOIN        = 0x03;
    /** A player left. */
    public static final byte TYPE_QUIT        = 0x04;
    /** A player died (death message). */
    public static final byte TYPE_DEATH       = 0x05;
    /** A player ran a command that should be broadcast (e.g. /me, /say). */
    public static final byte TYPE_COMMAND     = 0x06;

    public final byte   type;
    public final long   timestamp;
    public final String playerName;   // raw player name (no formatting)
    public final String message;      // the chat text / advancement title / death msg / command
    public final String serverName;   // originating server identifier

    public ChatSyncMessage(byte type, long timestamp,
                           String playerName, String message, String serverName) {
        this.type       = type;
        this.timestamp  = timestamp;
        this.playerName = playerName != null ? playerName : "";
        this.message    = message    != null ? message    : "";
        this.serverName = serverName != null ? serverName : "unknown";
    }

    // ── Encode ────────────────────────────────────────────────────────────────

    public byte[] encode() throws IOException {
        byte[] pnBytes  = playerName.getBytes(StandardCharsets.UTF_8);
        byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
        byte[] snBytes  = serverName.getBytes(StandardCharsets.UTF_8);

        ByteArrayOutputStream bos = new ByteArrayOutputStream(
                1 + 8 + 2 + pnBytes.length + 2 + msgBytes.length + 2 + snBytes.length);
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeByte(type);
        dos.writeLong(timestamp);
        dos.writeShort(pnBytes.length);
        dos.write(pnBytes);
        dos.writeShort(msgBytes.length);
        dos.write(msgBytes);
        dos.writeShort(snBytes.length);
        dos.write(snBytes);
        dos.flush();
        return bos.toByteArray();
    }

    // ── Decode ────────────────────────────────────────────────────────────────

    public static ChatSyncMessage decode(byte[] raw) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(raw));

        byte type      = dis.readByte();
        long timestamp = dis.readLong();

        int    pnLen      = dis.readShort() & 0xFFFF;
        byte[] pnBytes    = new byte[pnLen];
        dis.readFully(pnBytes);
        String playerName = new String(pnBytes, StandardCharsets.UTF_8);

        int    msgLen     = dis.readShort() & 0xFFFF;
        byte[] msgBytes   = new byte[msgLen];
        dis.readFully(msgBytes);
        String message    = new String(msgBytes, StandardCharsets.UTF_8);

        int    snLen      = dis.readShort() & 0xFFFF;
        byte[] snBytes    = new byte[snLen];
        dis.readFully(snBytes);
        String serverName = new String(snBytes, StandardCharsets.UTF_8);

        return new ChatSyncMessage(type, timestamp, playerName, message, serverName);
    }

    @Override
    public String toString() {
        String typeName = switch (type) {
            case TYPE_CHAT        -> "CHAT";
            case TYPE_ADVANCEMENT -> "ADVANCEMENT";
            case TYPE_JOIN        -> "JOIN";
            case TYPE_QUIT        -> "QUIT";
            case TYPE_DEATH       -> "DEATH";
            case TYPE_COMMAND     -> "COMMAND";
            default               -> "UNKNOWN(0x" + Integer.toHexString(type & 0xFF) + ")";
        };
        return "ChatSyncMessage{type=" + typeName
                + ", player=" + playerName
                + ", server=" + serverName
                + ", msg=" + message + "}";
    }
}
