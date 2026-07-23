package com.aicampions.poker.context;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.UUID;

/** Narrow context projection returned by the PostgreSQL lookup table. */
record UserContextRecord(
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

    UserContextRecord {
        new ContextKey(tenantId, productId, userId);
        if (contextVersion < 1) {
            throw new IllegalArgumentException("contextVersion must be positive");
        }
        if (effectiveAt == null || accountCreatedAt == null) {
            throw new IllegalArgumentException("context timestamps are required");
        }
    }

    UUID contextRecordId() {
        String eventName = String.join(
                ":",
                "postgres-user-context-v1",
                tenantId,
                productId,
                userId,
                Integer.toString(contextVersion),
                effectiveAt.toString());
        return TemporalJoinLogic.uuid5(URL_NAMESPACE, eventName);
    }

    ObjectNode toPayload() {
        ObjectNode payload = EventJson.MAPPER.createObjectNode();
        payload.put("user_id", userId);
        payload.put("context_version", contextVersion);
        payload.put("effective_at", effectiveAt.toString());
        payload.put("account_created_at", accountCreatedAt.toString());
        payload.put("country_bucket", countryBucket);
        payload.put("timezone", timezone);
        payload.put("acquisition_channel", acquisitionChannel);
        payload.put("kyc_level", kycLevel);
        payload.put("account_status", accountStatus);
        payload.put("bankroll_bucket", bankrollBucket);
        payload.put("preferred_stake_bucket", preferredStakeBucket);
        payload.put("skill_rating", skillRating);
        payload.put("device_id", deviceId);
        payload.put("network_cluster_id", networkClusterId);
        return payload;
    }
}
