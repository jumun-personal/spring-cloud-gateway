package com.jumunhasyeotjo.gateway.ratelimiter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueueItem {
    private Long userId;
    private String accessToken;
    private HttpRequestData httpRequest;
}
