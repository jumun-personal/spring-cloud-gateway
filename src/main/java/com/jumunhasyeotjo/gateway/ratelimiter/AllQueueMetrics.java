package com.jumunhasyeotjo.gateway.ratelimiter;

import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalQueueService;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalQueueService.QueueType;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalRateLimiterService;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.PaymentProviderRateLimiter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class AllQueueMetrics {

    private final GlobalQueueService globalQueueService;
    private final GlobalRateLimiterService globalRateLimiterService;
    private final List<PaymentProviderRateLimiter> paymentProviderRateLimiters;
    private final MeterRegistry meterRegistry;

    // 캐시 - 일반 큐
    private final AtomicLong cachedGlobalOrderQueueSize = new AtomicLong(0);
    private final AtomicLong cachedGlobalOtherQueueSize = new AtomicLong(0);
    // 캐시 - 재시도 큐
    private final AtomicLong cachedOrderRetryQueueSize = new AtomicLong(0);
    private final AtomicLong cachedOtherRetryQueueSize = new AtomicLong(0);
    // 캐시 - Rate Limit
    private final AtomicLong cachedGlobalWindowCount = new AtomicLong(0);
    private final Map<String, AtomicLong> cachedPgCurrentTokens = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("Initializing metrics for {} PG providers", paymentProviderRateLimiters.size());

        // 1. Global Queue - ORDER
        Gauge.builder("queue.waiting.users.global.order", cachedGlobalOrderQueueSize, AtomicLong::get)
                .description("Number of users waiting in global ORDER queue")
                .register(meterRegistry);

        // 2. Global Queue - OTHER
        Gauge.builder("queue.waiting.users.global.other", cachedGlobalOtherQueueSize, AtomicLong::get)
                .description("Number of users waiting in global OTHER queue")
                .register(meterRegistry);

        // 3. Retry Queue - ORDER
        Gauge.builder("queue.retry.order", cachedOrderRetryQueueSize, AtomicLong::get)
                .description("Number of requests in ORDER retry queue")
                .register(meterRegistry);

        // 4. Retry Queue - OTHER
        Gauge.builder("queue.retry.other", cachedOtherRetryQueueSize, AtomicLong::get)
                .description("Number of requests in OTHER retry queue")
                .register(meterRegistry);

        // 5. Global Rate Limit - Max (Leaky Bucket leak rate)
        Gauge.builder("rate.limit.global.max", globalRateLimiterService,
                        GlobalRateLimiterService::getCurrentLimit)
                .description("Global rate limit (leaky bucket leak rate)")
                .register(meterRegistry);

        // 6. Global Rate Limit - Current Water Level
        Gauge.builder("rate.limit.global.current", cachedGlobalWindowCount, AtomicLong::get)
                .description("Current water level in global leaky bucket")
                .register(meterRegistry);

        // 7. Global Rate Limit - Usage Ratio
        Gauge.builder("rate.limit.global.usage", this, metrics -> {
                    long current = cachedGlobalWindowCount.get();
                    int max = globalRateLimiterService.getCurrentLimit();
                    return max > 0 ? (double) current / max * 100 : 0;
                })
                .description("Global rate limit usage percentage")
                .baseUnit("percent")
                .register(meterRegistry);

        // 8. PG별 메트릭 (Leaky Bucket)
        for (PaymentProviderRateLimiter rateLimiter : paymentProviderRateLimiters) {
            String provider = rateLimiter.getProviderName();

            Gauge.builder("rate.limit.pg." + provider + ".max", rateLimiter,
                            PaymentProviderRateLimiter::getRateLimit)
                    .description("PG rate limit (leaky bucket leak rate)")
                    .register(meterRegistry);

            cachedPgCurrentTokens.put(provider, new AtomicLong(0));

            Gauge.builder("rate.limit.pg." + provider + ".current",
                            cachedPgCurrentTokens.get(provider),
                            AtomicLong::get)
                    .description("PG current available capacity (leaky bucket)")
                    .register(meterRegistry);

            Gauge.builder("rate.limit.pg." + provider + ".usage", this, metrics -> {
                        long current = cachedPgCurrentTokens.get(provider).get();
                        int max = rateLimiter.getRateLimit();
                        return max > 0 ? (double) (max - current) / max * 100 : 0;
                    })
                    .description("PG rate limit usage percentage")
                    .baseUnit("percent")
                    .register(meterRegistry);
        }

        log.info("✓ All metrics registered successfully");
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 2000)
    public void updateGlobalQueueSize() {
        // ORDER 큐
        globalQueueService.getQueueSize(QueueType.ORDER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> {
                            cachedGlobalOrderQueueSize.set(size);
                            log.debug("Global ORDER queue: {}", size);
                        },
                        error -> log.warn("Failed to update global ORDER queue size: {}", error.getMessage())
                );

        // OTHER 큐
        globalQueueService.getQueueSize(QueueType.OTHER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> {
                            cachedGlobalOtherQueueSize.set(size);
                            log.debug("Global OTHER queue: {}", size);
                        },
                        error -> log.warn("Failed to update global OTHER queue size: {}", error.getMessage())
                );

        // ORDER 재시도 큐
        globalQueueService.getRetryQueueSize(QueueType.ORDER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> {
                            cachedOrderRetryQueueSize.set(size);
                            log.debug("ORDER retry queue: {}", size);
                        },
                        error -> log.warn("Failed to update ORDER retry queue size: {}", error.getMessage())
                );

        // OTHER 재시도 큐
        globalQueueService.getRetryQueueSize(QueueType.OTHER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> {
                            cachedOtherRetryQueueSize.set(size);
                            log.debug("OTHER retry queue: {}", size);
                        },
                        error -> log.warn("Failed to update OTHER retry queue size: {}", error.getMessage())
                );
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 1000)
    public void updateGlobalWaterLevel() {
        globalRateLimiterService.getCurrentWindowCount()
                .timeout(Duration.ofSeconds(2))
                .subscribe(
                        waterLevel -> {
                            cachedGlobalWindowCount.set(waterLevel);
                            int capacity = globalRateLimiterService.getCurrentLimit();
                            double usage = capacity > 0 ? (double) waterLevel / capacity * 100 : 0;
                            log.info("Global leaky bucket: {}/{} ({}%)", waterLevel, capacity, String.format("%.1f", usage));
                        },
                        error -> log.warn("Failed to update global water level: {}", error.getMessage())
                );
    }

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
                                log.debug("PG {}: {}/{} used ({}%)", provider, used, max, String.format("%.1f", usage));
                            },
                            error -> log.warn("Failed to get PG {} tokens: {}", provider, error.getMessage())
                    );
        }
    }
}
