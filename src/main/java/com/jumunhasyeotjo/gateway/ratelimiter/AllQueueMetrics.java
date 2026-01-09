package com.jumunhasyeotjo.gateway.ratelimiter;

import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalQueueService;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalRateLimiterService;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.PaymentProviderRateLimiter;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.ReactiveRedisQueueService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class AllQueueMetrics {

    private final ReactiveRedisQueueService pgQueueService;
    private final GlobalQueueService globalQueueService;
    private final GlobalRateLimiterService globalRateLimiterService;
    private final List<PaymentProviderRateLimiter> paymentProviderRateLimiters;
    private final MeterRegistry meterRegistry;

    // 캐시
    private final AtomicLong cachedPgQueueSize = new AtomicLong(0);
    private final AtomicLong cachedGlobalQueueSize = new AtomicLong(0);
    private final AtomicLong cachedGlobalWindowCount = new AtomicLong(0);
    private final Map<String, AtomicLong> cachedPgCurrentTokens = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        // 1. PG Queue 대기열 (기존)
        Gauge.builder("queue.waiting.users.pg", cachedPgQueueSize, AtomicLong::get)
                .description("Number of users waiting in PG queue")
                .tag("type", "pg")
                .register(meterRegistry);

        // 2. Global Queue 대기열
        Gauge.builder("queue.waiting.users.global", cachedGlobalQueueSize, AtomicLong::get)
                .description("Number of users waiting in global queue")
                .tag("type", "global")
                .register(meterRegistry);

        // 3. Global Sliding Window Rate Limit 설정값
        Gauge.builder("rate.limit.global.max", globalRateLimiterService,
                        GlobalRateLimiterService::getCurrentLimit)
                .description("Global rate limit max (requests per second)")
                .tag("type", "sliding-window")
                .register(meterRegistry);

        // 4. Global Sliding Window 현재 사용량
        Gauge.builder("rate.limit.global.current", cachedGlobalWindowCount, AtomicLong::get)
                .description("Current requests in global sliding window")
                .tag("type", "sliding-window")
                .register(meterRegistry);

        // 5. PG별 토큰 Max & Current
        for (PaymentProviderRateLimiter rateLimiter : paymentProviderRateLimiters) {
            String provider = rateLimiter.getProviderName();

            // PG별 Max Token
            Gauge.builder("rate.limit.pg.max", rateLimiter,
                            PaymentProviderRateLimiter::getRateLimit)
                    .description("PG rate limit max tokens")
                    .tag("provider", provider)
                    .register(meterRegistry);

            // PG별 Current Token (캐시 초기화)
            cachedPgCurrentTokens.put(provider, new AtomicLong(0));

            Gauge.builder("rate.limit.pg.current",
                            cachedPgCurrentTokens.get(provider),
                            AtomicLong::get)
                    .description("PG current available tokens")
                    .tag("provider", provider)
                    .register(meterRegistry);
        }
    }

    // PG Queue 사이즈 업데이트
    @Scheduled(fixedDelay = 5000)
    public void updatePgQueueSize() {
        pgQueueService.getQueueSize()
                .timeout(Duration.ofSeconds(2))
                .subscribe(
                        size -> {
                            cachedPgQueueSize.set(size);
                            log.debug("PG queue size updated: {}", size);
                        },
                        error -> log.warn("Failed to update PG queue size: {}", error.getMessage())
                );
    }

    // Global Queue 사이즈 업데이트
    @Scheduled(fixedDelay = 5000)
    public void updateGlobalQueueSize() {
        globalQueueService.getQueueSize()
                .timeout(Duration.ofSeconds(2))
                .subscribe(
                        size -> {
                            cachedGlobalQueueSize.set(size);
                            log.debug("Global queue size updated: {}", size);
                        },
                        error -> log.warn("Failed to update global queue size: {}", error.getMessage())
                );
    }

    // Global Sliding Window 사용량 업데이트
    @Scheduled(fixedDelay = 1000)
    public void updateGlobalWindowCount() {
        globalRateLimiterService.getCurrentWindowCount()
                .timeout(Duration.ofSeconds(2))
                .subscribe(
                        count -> {
                            cachedGlobalWindowCount.set(count);
                            log.debug("Global window count updated: {}/{}",
                                    count, globalRateLimiterService.getCurrentLimit());
                        },
                        error -> log.warn("Failed to update global window count: {}", error.getMessage())
                );
    }

    // PG별 현재 토큰 업데이트
    @Scheduled(fixedDelay = 2000)
    public void updatePgCurrentTokens() {
        for (PaymentProviderRateLimiter rateLimiter : paymentProviderRateLimiters) {
            String provider = rateLimiter.getProviderName();

            rateLimiter.getAvailableTokens()
                    .timeout(Duration.ofSeconds(2))
                    .subscribe(
                            tokens -> {
                                cachedPgCurrentTokens.get(provider).set(tokens);
                                log.debug("PG {} available tokens: {}/{}",
                                        provider, tokens, rateLimiter.getRateLimit());
                            },
                            error -> log.warn("Failed to get PG {} available tokens: {}",
                                    provider, error.getMessage())
                    );
        }
    }
}