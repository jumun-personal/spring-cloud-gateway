package com.jumunhasyeotjo.gateway.ratelimiter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QueueItem {
    private String requestId;
    private String command;           // CREATE_ORDER, GET_PRODUCT 등
    private Long userId;
    private UUID resourceId;          // orderId, productId 등
    private String idempotencyKey;
    private int retryCount;
    private long originalTimestamp;
    private HttpRequestData httpRequest;
    private String accessToken;

    public static QueueItem createOrder(Long userId, UUID resourceId, String idempotencyKey, HttpRequestData httpRequestData, String accessToken) {
        return QueueItem.builder()
                .requestId(UUID.randomUUID().toString())
                .command("CREATE_ORDER")
                .userId(userId)
                .resourceId(resourceId)
                .idempotencyKey(idempotencyKey)
                .retryCount(0)
                .originalTimestamp(System.currentTimeMillis())
                .httpRequest(httpRequestData)
                .accessToken(accessToken)
                .build();
    }

    public static QueueItem createGeneric(String command, Long userId, UUID resourceId, HttpRequestData httpRequestData, String accessToken) {
        return QueueItem.builder()
                .requestId(UUID.randomUUID().toString())
                .command(command)
                .userId(userId)
                .resourceId(resourceId)
                .retryCount(0)
                .originalTimestamp(System.currentTimeMillis())
                .httpRequest(httpRequestData)
                .accessToken(accessToken)
                .build();
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    public boolean canRetry() {
        return this.retryCount < 1;
    }

    public boolean isOlderThan(long thresholdMs) {
        return System.currentTimeMillis() - this.originalTimestamp > thresholdMs;
    }
}
