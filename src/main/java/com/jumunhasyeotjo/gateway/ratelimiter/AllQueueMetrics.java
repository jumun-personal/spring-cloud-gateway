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
    // 캐시 - DLQ
    private final AtomicLong cachedDlqSize = new AtomicLong(0);
    // 캐시 - Rate Limit (Leaky Bucket)
    private final AtomicLong cachedGlobalWater = new AtomicLong(0);
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

        // 5. DLQ
        Gauge.builder("queue.dlq.size", cachedDlqSize, AtomicLong::get)
                .description("Number of requests in Dead Letter Queue")
                .register(meterRegistry);

        // 6. Global Rate Limit - Leaky Bucket Water
        Gauge.builder("rate.limit.global.water", cachedGlobalWater, AtomicLong::get)
                .description("Current water level in global leaky bucket")
                .register(meterRegistry);

        // 7. Global Rate Limit - Capacity
        Gauge.builder("rate.limit.global.capacity", globalRateLimiterService,
                        GlobalRateLimiterService::getCapacity)
                .description("Global leaky bucket capacity")
                .register(meterRegistry);

        // 8. Global Rate Limit - Rate
        Gauge.builder("rate.limit.global.rate", globalRateLimiterService,
                        GlobalRateLimiterService::getRate)
                .description("Global leaky bucket rate (per second)")
                .register(meterRegistry);

        // 9. Global Rate Limit - Usage Ratio
        Gauge.builder("rate.limit.global.usage", this, metrics -> {
                    long water = cachedGlobalWater.get();
                    int capacity = globalRateLimiterService.getCapacity();
                    return capacity > 0 ? (double) water / capacity * 100 : 0;
                })
                .description("Global leaky bucket usage percentage")
                .baseUnit("percent")
                .register(meterRegistry);

        // 10. PG별 메트릭
        for (PaymentProviderRateLimiter rateLimiter : paymentProviderRateLimiters) {
            String provider = rateLimiter.getProviderName();

            Gauge.builder("rate.limit.pg." + provider + ".rate", rateLimiter,
                            PaymentProviderRateLimiter::getRateLimit)
                    .description("PG leaky bucket rate")
                    .register(meterRegistry);

            cachedPgCurrentTokens.put(provider, new AtomicLong(0));

            Gauge.builder("rate.limit.pg." + provider + ".available",
                            cachedPgCurrentTokens.get(provider),
                            AtomicLong::get)
                    .description("PG current available tokens")
                    .register(meterRegistry);

            Gauge.builder("rate.limit.pg." + provider + ".usage", this, metrics -> {
                        long available = cachedPgCurrentTokens.get(provider).get();
                        int max = rateLimiter.getRateLimit();
                        return max > 0 ? (double) (max - available) / max * 100 : 0;
                    })
                    .description("PG rate limit usage percentage")
                    .baseUnit("percent")
                    .register(meterRegistry);
        }

        log.info("✓ All metrics registered successfully");
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 2000)
    public void updateQueueSizes() {
        // ORDER 큐
        globalQueueService.getQueueSize(QueueType.ORDER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> cachedGlobalOrderQueueSize.set(size),
                        error -> log.warn("Failed to update global ORDER queue size: {}", error.getMessage())
                );

        // OTHER 큐
        globalQueueService.getQueueSize(QueueType.OTHER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> cachedGlobalOtherQueueSize.set(size),
                        error -> log.warn("Failed to update global OTHER queue size: {}", error.getMessage())
                );

        // ORDER 재시도 큐
        globalQueueService.getRetryQueueSize(QueueType.ORDER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> cachedOrderRetryQueueSize.set(size),
                        error -> log.warn("Failed to update ORDER retry queue size: {}", error.getMessage())
                );

        // OTHER 재시도 큐
        globalQueueService.getRetryQueueSize(QueueType.OTHER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> cachedOtherRetryQueueSize.set(size),
                        error -> log.warn("Failed to update OTHER retry queue size: {}", error.getMessage())
                );

        // DLQ
        globalQueueService.getDlqSize()
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> {
                            cachedDlqSize.set(size);
                            if (size > 0) {
                                log.warn("DLQ size: {} - requires attention!", size);
                            }
                        },
                        error -> log.warn("Failed to update DLQ size: {}", error.getMessage())
                );
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 1000)
    public void updateGlobalWater() {
        globalRateLimiterService.getCurrentWater()
                .timeout(Duration.ofSeconds(2))
                .subscribe(
                        water -> {
                            cachedGlobalWater.set(water);
                            int capacity = globalRateLimiterService.getCapacity();
                            double usage = capacity > 0 ? (double) water / capacity * 100 : 0;
                            log.debug("Global Leaky Bucket: {}/{} ({}%)", water, capacity, String.format("%.1f", usage));
                        },
                        error -> log.warn("Failed to update global water: {}", error.getMessage())
                );
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 1500)
    public void updatePgTokens() {
        for (PaymentProviderRateLimiter rateLimiter : paymentProviderRateLimiters) {
            String provider = rateLimiter.getProviderName();

            rateLimiter.getAvailableTokens()
                    .timeout(Duration.ofSeconds(2))
                    .subscribe(
                            available -> {
                                cachedPgCurrentTokens.get(provider).set(available);
                                int max = rateLimiter.getRateLimit();
                                double usage = max > 0 ? (double) (max - available) / max * 100 : 0;
                                log.debug("PG {}: available={}, usage={}%", provider, available, String.format("%.1f", usage));
                            },
                            error -> log.warn("Failed to get PG {} tokens: {}", provider, error.getMessage())
                    );
        }
    }
}
