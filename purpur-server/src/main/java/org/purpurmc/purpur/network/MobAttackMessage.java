package org.purpurmc.purpur.network;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Binary message: slave player attacks a mob → forwarded to master for processing.
 *
 * Wire format (big-endian):
 *   [16 bytes] mobUUID       most-sig + least-sig longs
 *   [16 bytes] attackerUUID  the player's UUID
 *   [8  bytes] timestamp     System.currentTimeMillis()
 *   [4  bytes] damage        float
 *   [2  bytes] attackerNameLength
 *   [N  bytes] attackerName  UTF-8
 *   [2  bytes] worldNameLength
 *   [M  bytes] worldName     UTF-8
 *   [1  byte ] flags         bit0=isCritical, bit1=isSweeping
 */
public final class MobAttackMessage {

    public final UUID   mobUUID;
    public final UUID   attackerUUID;
    public final long   timestamp;
    public final float  damage;
    public final String attackerName;
    public final String worldName;
    public final byte   flags;

    public MobAttackMessage(UUID mobUUID, UUID attackerUUID, long timestamp,
                            float damage, String attackerName,
                            String worldName, byte flags) {
        this.mobUUID      = mobUUID;
        this.attackerUUID = attackerUUID;
        this.timestamp    = timestamp;
        this.damage       = damage;
        this.attackerName = attackerName != null ? attackerName : "";
        this.worldName    = worldName    != null ? worldName    : "world";
        this.flags        = flags;
    }

    // ── Encode ────────────────────────────────────────────────────────────────

    public byte[] encode() throws IOException {
        byte[] anBytes = attackerName.getBytes(StandardCharsets.UTF_8);
        byte[] wnBytes = worldName.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(64);
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(mobUUID.getMostSignificantBits());
        dos.writeLong(mobUUID.getLeastSignificantBits());
        dos.writeLong(attackerUUID.getMostSignificantBits());
        dos.writeLong(attackerUUID.getLeastSignificantBits());
        dos.writeLong(timestamp);
        dos.writeFloat(damage);
        dos.writeShort(anBytes.length);
        dos.write(anBytes);
        dos.writeShort(wnBytes.length);
        dos.write(wnBytes);
        dos.writeByte(flags);
        dos.flush();
        return bos.toByteArray();
    }

    // ── Decode ────────────────────────────────────────────────────────────────

    public static MobAttackMessage decode(byte[] raw) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(raw));
        UUID mobUUID      = new UUID(dis.readLong(), dis.readLong());
        UUID attackerUUID = new UUID(dis.readLong(), dis.readLong());
        long timestamp    = dis.readLong();
        float damage      = dis.readFloat();

        int    anLen      = dis.readShort() & 0xFFFF;
        byte[] anBytes    = new byte[anLen];
        dis.readFully(anBytes);
        String attackerName = new String(anBytes, StandardCharsets.UTF_8);

        int    wnLen   = dis.readShort() & 0xFFFF;
        byte[] wnBytes = new byte[wnLen];
        dis.readFully(wnBytes);
        String worldName = new String(wnBytes, StandardCharsets.UTF_8);

        byte flags = dis.readByte();

        return new MobAttackMessage(mobUUID, attackerUUID, timestamp,
                damage, attackerName, worldName, flags);
    }

    public boolean isCritical()  { return (flags & 0x01) != 0; }
    public boolean isSweeping()  { return (flags & 0x02) != 0; }

    @Override
    public String toString() {
        return "MobAttackMessage{mob=" + mobUUID + ", attacker=" + attackerName
                + ", dmg=" + damage + ", crit=" + isCritical() + "}";
    }
}
