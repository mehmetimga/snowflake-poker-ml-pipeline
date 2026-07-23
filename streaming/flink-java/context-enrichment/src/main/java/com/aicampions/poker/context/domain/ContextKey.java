package com.aicampions.poker.context.domain;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/** Tenant-safe Flink and PostgreSQL identity for one poker player. */
public final class ContextKey implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private String tenantId;
    private String productId;
    private String playerId;

    /** Required by Flink's POJO serializer. */
    public ContextKey() {}

    public ContextKey(String tenantId, String productId, String playerId) {
        this.tenantId = requireValue(tenantId, "tenantId");
        this.productId = requireValue(productId, "productId");
        this.playerId = requireValue(playerId, "playerId");
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

    public String getPlayerId() {
        return playerId;
    }

    public void setPlayerId(String playerId) {
        this.playerId = playerId;
    }

    public void validate() {
        requireValue(tenantId, "tenantId");
        requireValue(productId, "productId");
        requireValue(playerId, "playerId");
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ContextKey that)) {
            return false;
        }
        return Objects.equals(tenantId, that.tenantId)
                && Objects.equals(productId, that.productId)
                && Objects.equals(playerId, that.playerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenantId, productId, playerId);
    }

    @Override
    public String toString() {
        return "ContextKey[redacted]";
    }

    private static String requireValue(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-empty");
        }
        return value;
    }
}
