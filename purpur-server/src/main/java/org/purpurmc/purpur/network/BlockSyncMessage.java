package org.purpurmc.purpur.network;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Lightweight binary block-change message.
 *
 * Wire format (big-endian):
 *   [1 byte ] action     0x01=place, 0x02=break
 *   [4 bytes] x
 *   [4 bytes] y
 *   [4 bytes] z
 *   [8 bytes] timestamp  System.currentTimeMillis()
 *   [2 bytes] blockDataLength
 *   [N bytes] blockData  UTF-8 blockstate string, e.g. "minecraft:stone"
 *   [2 bytes] worldNameLength
 *   [M bytes] worldName  UTF-8
 *
 * Total ~30–60 bytes per block change.
 */
public final class BlockSyncMessage {

    public static final byte ACTION_PLACE = 0x01;
    public static final byte ACTION_BREAK = 0x02;

    public final byte   action;
    public final int    x;
    public final int    y;
    public final int    z;
    public final long   timestamp;
    public final String blockData;   // e.g. "minecraft:oak_log[axis=y]"
    public final String worldName;

    public BlockSyncMessage(byte action, int x, int y, int z, long timestamp,
                            String blockData, String worldName) {
        this.action    = action;
        this.x         = x;
        this.y         = y;
        this.z         = z;
        this.timestamp = timestamp;
        this.blockData = blockData;
        this.worldName = worldName;
    }

    // ── Encode ────────────────────────────────────────────────────────────────

    public byte[] encode() throws IOException {
        byte[] bdBytes  = blockData.getBytes(StandardCharsets.UTF_8);
        byte[] wnBytes  = worldName.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(32 + bdBytes.length + wnBytes.length);
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeByte(action);
        dos.writeInt(x);
        dos.writeInt(y);
        dos.writeInt(z);
        dos.writeLong(timestamp);
        dos.writeShort(bdBytes.length);
        dos.write(bdBytes);
        dos.writeShort(wnBytes.length);
        dos.write(wnBytes);
        dos.flush();
        return bos.toByteArray();
    }

    // ── Decode ────────────────────────────────────────────────────────────────

    public static BlockSyncMessage decode(byte[] raw) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(raw));
        byte   action    = dis.readByte();
        int    x         = dis.readInt();
        int    y         = dis.readInt();
        int    z         = dis.readInt();
        long   timestamp = dis.readLong();
        int    bdLen     = dis.readShort() & 0xFFFF;
        byte[] bdBytes   = new byte[bdLen];
        dis.readFully(bdBytes);
        String blockData = new String(bdBytes, StandardCharsets.UTF_8);
        int    wnLen     = dis.readShort() & 0xFFFF;
        byte[] wnBytes   = new byte[wnLen];
        dis.readFully(wnBytes);
        String worldName = new String(wnBytes, StandardCharsets.UTF_8);
        return new BlockSyncMessage(action, x, y, z, timestamp, blockData, worldName);
    }

    /** Convenience key used in conflict maps: "world/x/y/z" */
    public String posKey() {
        return worldName + "/" + x + "/" + y + "/" + z;
    }
}
