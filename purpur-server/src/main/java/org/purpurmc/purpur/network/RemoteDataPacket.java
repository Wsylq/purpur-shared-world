package org.purpurmc.purpur.network;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * RemoteDataPacket – binary protocol
 *
 * Wire format (all big-endian):
 *   [4 bytes] magic       = 0x52444154 ('RDAT')
 *   [1 byte ] op-code     (see OpCode enum)
 *   [4 bytes] request-id  (echo'd back in response so caller can match async)
 *   [2 bytes] key-length
 *   [N bytes] key         (UTF-8)
 *   [1 byte ] flags       bit0 = compressed
 *   [4 bytes] data-length
 *   [M bytes] data        (optionally gzip-compressed)
 *
 * HMAC-SHA256 of the whole packet (before the MAC itself) is appended:
 *   [32 bytes] HMAC-SHA256
 *
 * Total overhead per packet: 4+1+4+2+1+4+32 = 48 bytes + key + data
 */
public class RemoteDataPacket {

    public static final int MAGIC = 0x52444154; // 'RDAT'
    public static final int HEADER_SIZE = 4 + 1 + 4 + 2 + 1 + 4; // 16 bytes before data
    public static final int HMAC_SIZE = 32;

    // ── Op codes ──────────────────────────────────────────────────────────────
    public enum OpCode {
        // Client → Master
        PING              (0x01),
        AUTH              (0x02),
        GET               (0x10),
        PUT               (0x11),
        DELETE            (0x12),
        LIST              (0x13),
        BATCH_PUT         (0x14),
        CHUNK_INVALIDATE  (0x15),  // Client tells master: broadcast invalidation to other clients
        CHUNK_PUSH_ACK    (0x16),  // Client ACKs a push from master

        // Master → Client
        PONG              (0x81),
        AUTH_OK           (0x82),
        AUTH_FAIL         (0x83),
        DATA              (0x90),
        NOT_FOUND         (0x91),
        OK                (0x92),
        ERROR             (0x93),
        LIST_RESULT       (0x94),
        CHUNK_PUSH        (0x95);  // Master pushes chunk data to all other clients (real-time sync)

        public final byte code;

        OpCode(int code) {
            this.code = (byte) code;
        }

        public static OpCode fromByte(byte b) {
            for (OpCode op : values()) {
                if (op.code == b) return op;
            }
            throw new IllegalArgumentException("Unknown op-code: 0x" + Integer.toHexString(b & 0xFF));
        }
    }

    // ── Flags ─────────────────────────────────────────────────────────────────
    public static final byte FLAG_COMPRESSED = 0x01;

    // ── Fields ────────────────────────────────────────────────────────────────
    public final OpCode opCode;
    public final int requestId;
    public final String key;
    public final byte[] data;        // already decompressed
    public final boolean wasCompressed;

    public RemoteDataPacket(OpCode opCode, int requestId, String key, byte[] data) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.key = key != null ? key : "";
        this.data = data != null ? data : new byte[0];
        this.wasCompressed = false;
    }

    // internal – used by readFrom
    private RemoteDataPacket(OpCode opCode, int requestId, String key, byte[] data, boolean wasCompressed) {
        this.opCode = opCode;
        this.requestId = requestId;
        this.key = key;
        this.data = data;
        this.wasCompressed = wasCompressed;
    }

    // ── Serialisation ──────────────────────────────────────────────────────────

    /**
     * Serialise to wire format and write to the stream.
     * The HMAC is computed over everything except the HMAC bytes themselves.
     */
    public void writeTo(DataOutputStream out, HmacHelper hmac, boolean compress) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] payload = data;
        byte flags = 0;

        if (compress && payload.length > 256) {
            byte[] compressed = gzip(payload);
            if (compressed.length < payload.length) {
                payload = compressed;
                flags |= FLAG_COMPRESSED;
            }
        }

        // Build raw packet (without HMAC) into a byte buffer so we can sign it
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

    /**
     * Read and validate a packet from the stream.
     * Throws IOException if the HMAC doesn't match (tampered / wrong key).
     */
    public static RemoteDataPacket readFrom(DataInputStream in, HmacHelper hmac) throws IOException {
        ByteArrayOutputStream rawBuf = new ByteArrayOutputStream();
        DataOutputStream raw = new DataOutputStream(rawBuf);

        int magic = readIntRaw(in, raw);
        if (magic != MAGIC) {
            throw new IOException("Bad magic: 0x" + Integer.toHexString(magic));
        }

        byte opByte = readByteRaw(in, raw);
        OpCode op = OpCode.fromByte(opByte);

        int reqId = readIntRaw(in, raw);

        int keyLen = readShortRaw(in, raw) & 0xFFFF;
        byte[] keyBytes = new byte[keyLen];
        in.readFully(keyBytes);
        raw.write(keyBytes);

        byte flags = readByteRaw(in, raw);
        boolean compressed = (flags & FLAG_COMPRESSED) != 0;

        int dataLen = readIntRaw(in, raw);
        byte[] payload = new byte[dataLen];
        in.readFully(payload);
        raw.write(payload);
        raw.flush();

        // Now read HMAC
        byte[] receivedMac = new byte[HMAC_SIZE];
        in.readFully(receivedMac);

        // Verify
        byte[] expectedMac = hmac.sign(rawBuf.toByteArray());
        if (!constantTimeEquals(expectedMac, receivedMac)) {
            throw new IOException("HMAC mismatch – possible tampering or wrong secret key");
        }

        // Decompress if needed
        if (compressed) {
            payload = ungzip(payload);
        }

        return new RemoteDataPacket(op, reqId,
                new String(keyBytes, StandardCharsets.UTF_8), payload, compressed);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static int readIntRaw(DataInputStream in, DataOutputStream raw) throws IOException {
        int v = in.readInt();
        raw.writeInt(v);
        return v;
    }

    private static byte readByteRaw(DataInputStream in, DataOutputStream raw) throws IOException {
        byte v = in.readByte();
        raw.writeByte(v);
        return v;
    }

    private static short readShortRaw(DataInputStream in, DataOutputStream raw) throws IOException {
        short v = in.readShort();
        raw.writeShort(v);
        return v;
    }

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length / 2);
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(data);
        }
        return bos.toByteArray();
    }

    private static byte[] ungzip(byte[] data) throws IOException {
        try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(data))) {
            return gz.readAllBytes();
        }
    }

    /** Constant-time byte array comparison to prevent timing attacks */
    private static boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a.length != b.length) return false;
        int diff = 0;
        for (int i = 0; i < a.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }
}
