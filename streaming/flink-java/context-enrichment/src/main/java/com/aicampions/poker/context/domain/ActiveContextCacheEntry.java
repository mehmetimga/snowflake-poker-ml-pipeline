package com.aicampions.poker.context.domain;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/** Typed, versioned PostgreSQL context snapshot stored in Flink keyed state. */
public final class ActiveContextCacheEntry implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public static final int STATE_SCHEMA_VERSION = 1;

    private int stateSchemaVersion;
    private String contextRecordId;
    private String tenantId;
    private String productId;
    private String userId;
    private int contextVersion;
    private String effectiveAt;
    private String accountCreatedAt;
    private String countryBucket;
    private String timezone;
    private String acquisitionChannel;
    private String kycLevel;
    private String accountStatus;
    private String bankrollBucket;
    private String preferredStakeBucket;
    private double skillRating;
    private String deviceId;
    private String networkClusterId;
    private long loadedAtMs;

    /** Required by Flink's POJO serializer. */
    public ActiveContextCacheEntry() {}

    public static ActiveContextCacheEntry from(
            UserContextRecord record, long loadedAtMs) {
        ActiveContextCacheEntry entry = new ActiveContextCacheEntry();
        entry.stateSchemaVersion = STATE_SCHEMA_VERSION;
        entry.contextRecordId = record.contextRecordId().toString();
        entry.tenantId = record.tenantId();
        entry.productId = record.productId();
        entry.userId = record.userId();
        entry.contextVersion = record.contextVersion();
        entry.effectiveAt = record.effectiveAt().toString();
        entry.accountCreatedAt =
                record.accountCreatedAt().toString();
        entry.countryBucket = record.countryBucket();
        entry.timezone = record.timezone();
        entry.acquisitionChannel = record.acquisitionChannel();
        entry.kycLevel = record.kycLevel();
        entry.accountStatus = record.accountStatus();
        entry.bankrollBucket = record.bankrollBucket();
        entry.preferredStakeBucket = record.preferredStakeBucket();
        entry.skillRating = record.skillRating();
        entry.deviceId = record.deviceId();
        entry.networkClusterId = record.networkClusterId();
        entry.loadedAtMs = loadedAtMs;
        entry.validate();
        return entry;
    }

    public UserContextRecord toRecord() {
        validate();
        return new UserContextRecord(
                tenantId,
                productId,
                userId,
                contextVersion,
                Instant.parse(effectiveAt),
                Instant.parse(accountCreatedAt),
                countryBucket,
                timezone,
                acquisitionChannel,
                kycLevel,
                accountStatus,
                bankrollBucket,
                preferredStakeBucket,
                skillRating,
                deviceId,
                networkClusterId);
    }

    public void validate() {
        if (stateSchemaVersion != STATE_SCHEMA_VERSION) {
            throw new IllegalStateException(
                    "unsupported active-context state schema version "
                            + stateSchemaVersion);
        }
        UserContextRecord record = uncheckedRecord();
        String expectedRecordId = record.contextRecordId().toString();
        if (!expectedRecordId.equals(contextRecordId)) {
            throw new IllegalStateException(
                    "active-context state record ID is inconsistent");
        }
        UUID.fromString(contextRecordId);
        if (loadedAtMs < 0L) {
            throw new IllegalStateException(
                    "active-context loadedAtMs must be non-negative");
        }
    }

    public boolean isFresh(long nowMs, long refreshAfterMs) {
        validate();
        return refreshAfterMs > 0L
                && nowMs >= loadedAtMs
                && nowMs - loadedAtMs < refreshAfterMs;
    }

    public boolean isEffectiveFor(long playedAtMs) {
        validate();
        return Instant.parse(effectiveAt).toEpochMilli() <= playedAtMs;
    }

    public boolean shouldBeReplacedBy(
            ActiveContextCacheEntry candidate) {
        validate();
        candidate.validate();
        if (!new ContextKey(tenantId, productId, userId).equals(
                new ContextKey(
                        candidate.tenantId,
                        candidate.productId,
                        candidate.userId))) {
            throw new IllegalArgumentException(
                    "candidate cache entry has a different context key");
        }
        int effectiveOrder = Instant.parse(candidate.effectiveAt)
                .compareTo(Instant.parse(effectiveAt));
        if (effectiveOrder != 0) {
            return effectiveOrder > 0;
        }
        return candidate.contextVersion >= contextVersion;
    }

    private UserContextRecord uncheckedRecord() {
        return new UserContextRecord(
                tenantId,
                productId,
                userId,
                contextVersion,
                Instant.parse(effectiveAt),
                Instant.parse(accountCreatedAt),
                countryBucket,
                timezone,
                acquisitionChannel,
                kycLevel,
                accountStatus,
                bankrollBucket,
                preferredStakeBucket,
                skillRating,
                deviceId,
                networkClusterId);
    }

    public int getStateSchemaVersion() {
        return stateSchemaVersion;
    }

    public void setStateSchemaVersion(int stateSchemaVersion) {
        this.stateSchemaVersion = stateSchemaVersion;
    }

    public String getContextRecordId() {
        return contextRecordId;
    }

    public void setContextRecordId(String contextRecordId) {
        this.contextRecordId = contextRecordId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getContextVersion() {
        return contextVersion;
    }

    public void setContextVersion(int contextVersion) {
        this.contextVersion = contextVersion;
    }

    public String getEffectiveAt() {
        return effectiveAt;
    }

    public void setEffectiveAt(String effectiveAt) {
        this.effectiveAt = effectiveAt;
    }

    public String getAccountCreatedAt() {
        return accountCreatedAt;
    }

    public void setAccountCreatedAt(String accountCreatedAt) {
        this.accountCreatedAt = accountCreatedAt;
    }

    public String getCountryBucket() {
        return countryBucket;
    }

    public void setCountryBucket(String countryBucket) {
        this.countryBucket = countryBucket;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getAcquisitionChannel() {
        return acquisitionChannel;
    }

    public void setAcquisitionChannel(String acquisitionChannel) {
        this.acquisitionChannel = acquisitionChannel;
    }

    public String getKycLevel() {
        return kycLevel;
    }

    public void setKycLevel(String kycLevel) {
        this.kycLevel = kycLevel;
    }

    public String getAccountStatus() {
        return accountStatus;
    }

    public void setAccountStatus(String accountStatus) {
        this.accountStatus = accountStatus;
    }

    public String getBankrollBucket() {
        return bankrollBucket;
    }

    public void setBankrollBucket(String bankrollBucket) {
        this.bankrollBucket = bankrollBucket;
    }

    public String getPreferredStakeBucket() {
        return preferredStakeBucket;
    }

    public void setPreferredStakeBucket(String preferredStakeBucket) {
        this.preferredStakeBucket = preferredStakeBucket;
    }

    public double getSkillRating() {
        return skillRating;
    }

    public void setSkillRating(double skillRating) {
        this.skillRating = skillRating;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getNetworkClusterId() {
        return networkClusterId;
    }

    public void setNetworkClusterId(String networkClusterId) {
        this.networkClusterId = networkClusterId;
    }

    public long getLoadedAtMs() {
        return loadedAtMs;
    }

    public void setLoadedAtMs(long loadedAtMs) {
        this.loadedAtMs = loadedAtMs;
    }
}
