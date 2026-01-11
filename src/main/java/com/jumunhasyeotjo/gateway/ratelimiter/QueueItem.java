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

    public QueueItem(Long userId, String accessToken, HttpRequestData httpRequest) {
        this.requestId = UUID.randomUUID().toString();
        this.userId = userId;
        this.accessToken = accessToken;
        this.httpRequest = httpRequest;
    }
}
