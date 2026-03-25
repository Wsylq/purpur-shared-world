package org.purpurmc.purpur.network;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Binary message carrying a mob's replicated state.
 *
 * Wire format (big-endian):
 *   [1  byte ] type         TYPE_FULL_STATE | TYPE_HEALTH_UPDATE
 *   [16 bytes] entityUUID   most-sig + least-sig longs
 *   [8  bytes] timestamp    System.currentTimeMillis()
 *   [2  bytes] entityTypeLength
 *   [N  bytes] entityType   UTF-8  e.g. "minecraft:zombie"
 *   [2  bytes] worldNameLength
 *   [M  bytes] worldName    UTF-8
 *   [8  bytes] x            double
 *   [8  bytes] y            double
 *   [8  bytes] z            double
 *   [4  bytes] yaw          float
 *   [4  bytes] pitch        float
 *   [8  bytes] velX         double
 *   [8  bytes] velY         double
 *   [8  bytes] velZ         double
 *   [4  bytes] health       float
 *   [4  bytes] maxHealth    float
 *   [1  byte ] flags        bit0=dead, bit1=onGround
 */
public final class MobSyncMessage {

    public static final byte TYPE_FULL_STATE    = 0x01;
    public static final byte TYPE_HEALTH_UPDATE = 0x02;

    public final byte   type;
    public final UUID   entityUUID;
    public final long   timestamp;
    public final String entityType;   // e.g. "minecraft:zombie"
    public final String worldName;
    public final double x, y, z;
    public final float  yaw, pitch;
    public final double velX, velY, velZ;
    public final float  health, maxHealth;
    public final byte   flags;         // bit0 = dead, bit1 = onGround

    public MobSyncMessage(byte type, UUID entityUUID, long timestamp,
                          String entityType, String worldName,
                          double x, double y, double z,
                          float yaw, float pitch,
                          double velX, double velY, double velZ,
                          float health, float maxHealth, byte flags) {
        this.type       = type;
        this.entityUUID = entityUUID;
        this.timestamp  = timestamp;
        this.entityType = entityType != null ? entityType : "minecraft:pig";
        this.worldName  = worldName  != null ? worldName  : "world";
        this.x = x; this.y = y; this.z = z;
        this.yaw = yaw; this.pitch = pitch;
        this.velX = velX; this.velY = velY; this.velZ = velZ;
        this.health    = health;
        this.maxHealth = maxHealth;
        this.flags     = flags;
    }

    // ── Encode ────────────────────────────────────────────────────────────────

    public byte[] encode() throws IOException {
        byte[] etBytes = entityType.getBytes(StandardCharsets.UTF_8);
        byte[] wnBytes = worldName.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeByte(type);
        dos.writeLong(entityUUID.getMostSignificantBits());
        dos.writeLong(entityUUID.getLeastSignificantBits());
        dos.writeLong(timestamp);
        dos.writeShort(etBytes.length);
        dos.write(etBytes);
        dos.writeShort(wnBytes.length);
        dos.write(wnBytes);
        dos.writeDouble(x);
        dos.writeDouble(y);
        dos.writeDouble(z);
        dos.writeFloat(yaw);
        dos.writeFloat(pitch);
        dos.writeDouble(velX);
        dos.writeDouble(velY);
        dos.writeDouble(velZ);
        dos.writeFloat(health);
        dos.writeFloat(maxHealth);
        dos.writeByte(flags);
        dos.flush();
        return bos.toByteArray();
    }

    // ── Decode ────────────────────────────────────────────────────────────────

    public static MobSyncMessage decode(byte[] raw) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(raw));
        byte type      = dis.readByte();
        long msb       = dis.readLong();
        long lsb       = dis.readLong();
        UUID uuid      = new UUID(msb, lsb);
        long timestamp = dis.readLong();

        int    etLen      = dis.readShort() & 0xFFFF;
        byte[] etBytes    = new byte[etLen];
        dis.readFully(etBytes);
        String entityType = new String(etBytes, StandardCharsets.UTF_8);

        int    wnLen      = dis.readShort() & 0xFFFF;
        byte[] wnBytes    = new byte[wnLen];
        dis.readFully(wnBytes);
        String worldName  = new String(wnBytes, StandardCharsets.UTF_8);

        double x     = dis.readDouble();
        double y     = dis.readDouble();
        double z     = dis.readDouble();
        float  yaw   = dis.readFloat();
        float  pitch = dis.readFloat();
        double velX  = dis.readDouble();
        double velY  = dis.readDouble();
        double velZ  = dis.readDouble();
        float  health    = dis.readFloat();
        float  maxHealth = dis.readFloat();
        byte   flags     = dis.readByte();

        return new MobSyncMessage(type, uuid, timestamp, entityType, worldName,
                x, y, z, yaw, pitch, velX, velY, velZ, health, maxHealth, flags);
    }

    public boolean isDead()     { return (flags & 0x01) != 0; }
    public boolean isOnGround() { return (flags & 0x02) != 0; }

    @Override
    public String toString() {
        return "MobSyncMessage{type=" + type + ", uuid=" + entityUUID
                + ", pos=(" + String.format("%.1f,%.1f,%.1f", x, y, z)
                + "), hp=" + health + "/" + maxHealth + "}";
    }
}
