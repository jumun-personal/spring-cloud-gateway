package com.jumunhasyeotjo.gateway.ratelimiter.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitResponse {
    private boolean allowed;
    private boolean queued;
    private Long queuePosition;
    private int currentLimit;
    private String queueType;
    private String message;
}
