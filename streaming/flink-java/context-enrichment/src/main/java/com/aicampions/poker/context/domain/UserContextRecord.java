package com.aicampions.poker.context.domain;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.UUID;

/** Narrow context projection returned by the governed user-context table. */
public record UserContextRecord(
        String tenantId,
        String productId,
        String userId,
        int contextVersion,
        Instant effectiveAt,
        Instant accountCreatedAt,
        String countryBucket,
        String timezone,
        String acquisitionChannel,
        String kycLevel,
        String accountStatus,
        String bankrollBucket,
        String preferredStakeBucket,
        double skillRating,
        String deviceId,
        String networkClusterId) {
    private static final UUID URL_NAMESPACE =
            UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");

    public UserContextRecord {
        new ContextKey(tenantId, productId, userId);
        if (contextVersion < 1) {
            throw new IllegalArgumentException("contextVersion must be positive");
        }
        if (effectiveAt == null || accountCreatedAt == null) {
            throw new IllegalArgumentException("context timestamps are required");
        }
    }

    public UUID contextRecordId() {
        String eventName = String.join(
                ":",
                "poker-user-context-v1",
                tenantId,
                productId,
                userId,
                Integer.toString(contextVersion),
                effectiveAt.toString());
        return uuid5(URL_NAMESPACE, eventName);
    }

    private static UUID uuid5(UUID namespace, String name) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            ByteBuffer namespaceBytes = ByteBuffer.allocate(16);
            namespaceBytes.putLong(namespace.getMostSignificantBits());
            namespaceBytes.putLong(namespace.getLeastSignificantBits());
            sha1.update(namespaceBytes.array());
            byte[] digest = sha1.digest(name.getBytes(StandardCharsets.UTF_8));
            digest[6] = (byte) ((digest[6] & 0x0f) | 0x50);
            digest[8] = (byte) ((digest[8] & 0x3f) | 0x80);
            ByteBuffer uuidBytes = ByteBuffer.wrap(digest, 0, 16);
            return new UUID(uuidBytes.getLong(), uuidBytes.getLong());
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-1 is unavailable", error);
        }
    }
}
