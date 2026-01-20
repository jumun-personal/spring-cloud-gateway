package com.jumunhasyeotjo.gateway.ratelimiter.dto;

import com.jumunhasyeotjo.gateway.ratelimiter.HttpRequestData;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitRequest {
    private String path;
    private Long userId;
    private String accessToken;
    private HttpRequestData httpRequest;
    private String provider;
}
