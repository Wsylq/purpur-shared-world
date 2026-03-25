package org.purpurmc.purpur.network;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * RemoteDataPacket – binary protocol (v5, block + chat sync)
 *
 * Wire format (all big-endian):
 *   [4 bytes] magic       = 0x52444154 ('RDAT')
 *   [1 byte ] op-code
 *   [4 bytes] request-id  (0 = unsolicited server push)
 *   [2 bytes] key-length
 *   [N bytes] key (UTF-8)
 *   [1 byte ] flags  bit0=compressed
 *   [4 bytes] data-length
 *   [M bytes] data
 *   [32 bytes] HMAC-SHA256
 *
 * OpCodes added in v5:
 *   CHAT_ACTION  (0x18) – slave → master: encoded ChatSyncMessage
 *   CHAT_PUSH    (0x97) – master → slaves: encoded ChatSyncMessage (requestId=0)
 */
public class RemoteDataPacket {

    public static final int MAGIC     = 0x52444154;
    public static final int HMAC_SIZE = 32;

    public enum OpCode {
        // Client → Master
        PING             (0x01),
        AUTH             (0x02),
        GET              (0x10),
        PUT              (0x11),
        DELETE           (0x12),
        LIST             (0x13),
        BATCH_PUT        (0x14),
        CHUNK_INVALIDATE (0x15),
        CHUNK_PUSH_ACK   (0x16),
        BLOCK_ACTION     (0x17),   // slave sends a block place/break to master
        CHAT_ACTION      (0x18),   // slave sends a chat/advancement/join/quit/death event to master

        // Master → Client
        PONG             (0x81),
        AUTH_OK          (0x82),
        AUTH_FAIL        (0x83),
        DATA             (0x90),
        NOT_FOUND        (0x91),
        OK               (0x92),
        ERROR            (0x93),
        LIST_RESULT      (0x94),
        CHUNK_PUSH       (0x95),
        BLOCK_PUSH       (0x96),  // master broadcasts applied block state to all slaves
        CHAT_PUSH        (0x97);  // master broadcasts chat/advancement/join/quit/death to all slaves

        public final byte code;
        OpCode(int code) { this.code = (byte) code; }

        public static OpCode fromByte(byte b) {
            for (OpCode op : values()) if (op.code == b) return op;
            throw new IllegalArgumentException("Unknown op-code: 0x" + Integer.toHexString(b & 0xFF));
        }
    }

    public static final byte FLAG_COMPRESSED = 0x01;

    public final OpCode  opCode;
    public final int     requestId;
    public final String  key;
    public final byte[]  data;
    public final boolean wasCompressed;

    public RemoteDataPacket(OpCode opCode, int requestId, String key, byte[] data) {
        this.opCode        = opCode;
        this.requestId     = requestId;
        this.key           = key  != null ? key  : "";
        this.data          = data != null ? data : new byte[0];
        this.wasCompressed = false;
    }

    private RemoteDataPacket(OpCode op, int id, String key, byte[] data, boolean compressed) {
        this.opCode = op; this.requestId = id; this.key = key;
        this.data = data; this.wasCompressed = compressed;
    }

    // ── Serialisation ─────────────────────────────────────────────────────────

    public void writeTo(DataOutputStream out, HmacHelper hmac, boolean compress) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] payload  = data;
        byte   flags    = 0;

        if (compress && payload.length > 256) {
            byte[] c = gzip(payload);
            if (c.length < payload.length) { payload = c; flags |= FLAG_COMPRESSED; }
        }

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(buf);
        d.writeInt(MAGIC);
        d.writeByte(opCode.code);
        d.writeInt(requestId);
        d.writeShort(keyBytes.length);
        d.write(keyBytes);
        d.writeByte(flags);
        d.writeInt(payload.length);
        d.write(payload);
        d.flush();

        byte[] raw = buf.toByteArray();
        byte[] mac = hmac.sign(raw);
        out.write(raw);
        out.write(mac);
        out.flush();
    }

    public static RemoteDataPacket readFrom(DataInputStream in, HmacHelper hmac) throws IOException {
        ByteArrayOutputStream rawBuf = new ByteArrayOutputStream();
        DataOutputStream raw = new DataOutputStream(rawBuf);

        int magic = readIntRaw(in, raw);
        if (magic != MAGIC) throw new IOException("Bad magic: 0x" + Integer.toHexString(magic));

        OpCode op   = OpCode.fromByte(readByteRaw(in, raw));
        int    id   = readIntRaw(in, raw);
        int    kLen = readShortRaw(in, raw) & 0xFFFF;

        byte[] keyBytes = new byte[kLen];
        in.readFully(keyBytes);
        raw.write(keyBytes);

        byte    flags      = readByteRaw(in, raw);
        boolean compressed = (flags & FLAG_COMPRESSED) != 0;
        int     dLen       = readIntRaw(in, raw);

        byte[] payload = new byte[dLen];
        in.readFully(payload);
        raw.write(payload);
        raw.flush();

        byte[] receivedMac = new byte[HMAC_SIZE];
        in.readFully(receivedMac);
        if (!constantTimeEquals(hmac.sign(rawBuf.toByteArray()), receivedMac))
            throw new IOException("HMAC mismatch");

        if (compressed) payload = ungzip(payload);
        return new RemoteDataPacket(op, id, new String(keyBytes, StandardCharsets.UTF_8), payload, compressed);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static int   readIntRaw  (DataInputStream in, DataOutputStream r) throws IOException { int   v=in.readInt();   r.writeInt(v);   return v; }
    private static byte  readByteRaw (DataInputStream in, DataOutputStream r) throws IOException { byte  v=in.readByte();  r.writeByte(v);  return v; }
    private static short readShortRaw(DataInputStream in, DataOutputStream r) throws IOException { short v=in.readShort(); r.writeShort(v); return v; }

    private static byte[] gzip(byte[] d) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream(d.length / 2);
        try (GZIPOutputStream g = new GZIPOutputStream(b)) { g.write(d); }
        return b.toByteArray();
    }
    private static byte[] ungzip(byte[] d) throws IOException {
        try (GZIPInputStream g = new GZIPInputStream(new ByteArrayInputStream(d))) { return g.readAllBytes(); }
    }
    private static boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a.length != b.length) return false;
        int diff = 0;
        for (int i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
        return diff == 0;
    }
}
