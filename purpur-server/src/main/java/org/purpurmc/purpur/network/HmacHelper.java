package org.purpurmc.purpur.network;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * Thread-safe HMAC-SHA256 helper.
 *
 * Each call to sign() creates a fresh Mac instance because Mac is NOT thread-safe.
 * The key is stored once and reused.
 */
public class HmacHelper {

    private static final String ALGORITHM = "HmacSHA256";
    private final SecretKeySpec keySpec;

    public HmacHelper(String secretKey) {
        byte[] keyBytes = secretKey.getBytes(StandardCharsets.UTF_8);
        this.keySpec = new SecretKeySpec(keyBytes, ALGORITHM);
    }

    /**
     * Sign the given data and return the 32-byte HMAC-SHA256.
     */
    public byte[] sign(byte[] data) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(keySpec);
            return mac.doFinal(data);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            // Should never happen on a standard JVM
            throw new RuntimeException("HMAC-SHA256 unavailable", e);
        }
    }
}
