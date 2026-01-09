package com.jumunhasyeotjo.gateway.ratelimiter.pg;

import reactor.core.publisher.Mono;

public interface PaymentProviderRateLimiter {

    /**
     * 토큰 소비 시도
     */
    Mono<Boolean> tryConsume();

    /**
     * PG사 이름
     */
    String getProviderName();

    /**
     * 초당 요청 제한
     */
    int getRateLimit();

    /**
     * 현재 사용 가능한 토큰 수
     */
    Mono<Long> getAvailableTokens();
}
