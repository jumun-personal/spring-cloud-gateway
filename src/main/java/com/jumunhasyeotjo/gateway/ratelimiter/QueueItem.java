package com.jumunhasyeotjo.gateway.ratelimiter;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
public class QueueItem {
    private String requestId;
    private Long userId;
    private String accessToken;
    private HttpRequestData httpRequest;
    private int retryCount;
    private long originalTimestamp;

    public QueueItem(Long userId, String accessToken, HttpRequestData httpRequest) {
        this.requestId = UUID.randomUUID().toString();
        this.userId = userId;
        this.accessToken = accessToken;
        this.httpRequest = httpRequest;
        this.retryCount = 0;
        this.originalTimestamp = System.currentTimeMillis();
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
