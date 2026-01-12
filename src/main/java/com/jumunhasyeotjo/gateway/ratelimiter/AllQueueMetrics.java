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
        log.info("Initializing metrics for {} PG providers", paymentProviderRateLimiters.size());

        // 1. PG Queue 대기열
        Gauge.builder("queue.waiting.users.pg", cachedPgQueueSize, AtomicLong::get)
                .description("Number of users waiting in PG queue")
                .tag("type", "pg")
                .register(meterRegistry);

        // 2. Global Queue 대기열
        Gauge.builder("queue.waiting.users.global", cachedGlobalQueueSize, AtomicLong::get)
                .description("Number of users waiting in global queue")
                .tag("type", "global")
                .register(meterRegistry);

        // 3. Global Rate Limit - Max (동적 변경)
        Gauge.builder("rate.limit.global.max", globalRateLimiterService,
                        GlobalRateLimiterService::getCurrentLimit)
                .description("Global rate limit max (sliding window)")
                .tag("type", "sliding-window")
                .register(meterRegistry);

        // 4. Global Rate Limit - Current Usage
        Gauge.builder("rate.limit.global.current", cachedGlobalWindowCount, AtomicLong::get)
                .description("Current requests in global sliding window")
                .tag("type", "sliding-window")
                .register(meterRegistry);

        // 5. Global Rate Limit - Usage Ratio (추가!)
        Gauge.builder("rate.limit.global.usage", this, metrics -> {
                    long current = cachedGlobalWindowCount.get();
                    int max = globalRateLimiterService.getCurrentLimit();
                    return max > 0 ? (double) current / max * 100 : 0;
                })
                .description("Global rate limit usage percentage")
                .tag("type", "sliding-window")
                .baseUnit("percent")
                .register(meterRegistry);

        // 6. PG별 메트릭
        for (PaymentProviderRateLimiter rateLimiter : paymentProviderRateLimiters) {
            String provider = rateLimiter.getProviderName();

            // PG Max Limit
            Gauge.builder("rate.limit.pg.max", rateLimiter,
                            PaymentProviderRateLimiter::getRateLimit)
                    .description("PG rate limit max tokens")
                    .tag("provider", provider)
                    .register(meterRegistry);

            // PG Current Available Tokens
            cachedPgCurrentTokens.put(provider, new AtomicLong(0));

            Gauge.builder("rate.limit.pg.current",
                            cachedPgCurrentTokens.get(provider),
                            AtomicLong::get)
                    .description("PG current available tokens")
                    .tag("provider", provider)
                    .register(meterRegistry);

            // PG Usage Ratio (추가!)
            Gauge.builder("rate.limit.pg.usage", this, metrics -> {
                        long current = cachedPgCurrentTokens.get(provider).get();
                        int max = rateLimiter.getRateLimit();
                        return max > 0 ? (double) (max - current) / max * 100 : 0;
                    })
                    .description("PG rate limit usage percentage")
                    .tag("provider", provider)
                    .baseUnit("percent")
                    .register(meterRegistry);
        }

        log.info("✓ All metrics registered successfully");
    }

    /**
     * PG Queue 사이즈 업데이트 (5초마다)
     */
    @Scheduled(fixedDelay = 5000, initialDelay = 2000)
    public void updatePgQueueSize() {
        pgQueueService.getQueueSize()
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> {
                            cachedPgQueueSize.set(size);
                            log.info(" PG queue: {}", size);
                        },
                        error -> log.warn("Failed to update PG queue size: {}", error.getMessage())
                );
    }

    /**
     * Global Queue 사이즈 업데이트 (5초마다)
     */
    @Scheduled(fixedDelay = 5000, initialDelay = 2000)
    public void updateGlobalQueueSize() {
        globalQueueService.getQueueSize()
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> {
                            cachedGlobalQueueSize.set(size);
                            log.info(" Global queue: {}", size);
                        },
                        error -> log.warn("Failed to update global queue size: {}", error.getMessage())
                );
    }

    /**
     * Global Sliding Window 사용량 업데이트 (1초마다)
     */
    @Scheduled(fixedDelay = 1000, initialDelay = 1000)
    public void updateGlobalWindowCount() {
        globalRateLimiterService.getCurrentWindowCount()
                .timeout(Duration.ofSeconds(2))
                .subscribe(
                        count -> {
                            cachedGlobalWindowCount.set(count);
                            int limit = globalRateLimiterService.getCurrentLimit();
                            double usage = limit > 0 ? (double) count / limit * 100 : 0;

                            log.info(" Global: {}/{} ({}%)", count, limit, String.format("%.1f", usage));
                        },
                        error -> log.warn("Failed to update global window count: {}", error.getMessage())
                );
    }

    /**
     * PG별 현재 토큰 업데이트 (2초마다)
     */
    @Scheduled(fixedDelay = 2000, initialDelay = 1500)
    public void updatePgCurrentTokens() {
        for (PaymentProviderRateLimiter rateLimiter : paymentProviderRateLimiters) {
            String provider = rateLimiter.getProviderName();

            rateLimiter.getAvailableTokens()
                    .timeout(Duration.ofSeconds(2))
                    .subscribe(
                            available -> {
                                cachedPgCurrentTokens.get(provider).set(available);
                                int max = rateLimiter.getRateLimit();
                                long used = max - available;
                                double usage = max > 0 ? (double) used / max * 100 : 0;

                                log.info(" PG {}: {}/{} used ({}%)",
                                        provider, used, max, String.format("%.1f", usage));
                            },
                            error -> log.warn("Failed to get PG {} tokens: {}",
                                    provider, error.getMessage())
                    );
        }
    }
}