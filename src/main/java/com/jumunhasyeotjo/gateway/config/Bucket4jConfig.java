package com.jumunhasyeotjo.gateway.config;

import org.springframework.context.annotation.Configuration;

/**
 * 기존 Bucket4j 설정은 Leaky Bucket 알고리즘으로 대체되었습니다.
 *
 * Rate Limiting은 이제 Redis Hash 기반의 Leaky Bucket을 사용합니다:
 * - GlobalRateLimiterService: leaky:global:bucket
 * - TossPaymentRateLimiter: leaky:pg:toss
 *
 * 이 설정 클래스는 향후 삭제 가능합니다 (Bucket4j 의존성 제거 시).
 */
@Configuration
public class Bucket4jConfig {
    // Bucket4j 및 Redisson 빈 제거됨 - Leaky Bucket으로 대체
}