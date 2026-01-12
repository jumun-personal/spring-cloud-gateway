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

    // 캐시
    private final AtomicLong cachedGlobalOrderQueueSize = new AtomicLong(0);
    private final AtomicLong cachedGlobalOtherQueueSize = new AtomicLong(0);
    private final AtomicLong cachedGlobalWindowCount = new AtomicLong(0);
    private final Map<String, AtomicLong> cachedPgCurrentTokens = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("Initializing metrics for {} PG providers", paymentProviderRateLimiters.size());

        // 1. Global Queue - ORDER
        Gauge.builder("queue.waiting.users.global", cachedGlobalOrderQueueSize, AtomicLong::get)
                .description("Number of users waiting in global ORDER queue")
                .tag("type", "order")
                .register(meterRegistry);

        // 2. Global Queue - OTHER
        Gauge.builder("queue.waiting.users.global", cachedGlobalOtherQueueSize, AtomicLong::get)
                .description("Number of users waiting in global OTHER queue")
                .tag("type", "other")
                .register(meterRegistry);

        // 3. Global Rate Limit - Max
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

        // 5. Global Rate Limit - Usage Ratio
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

            Gauge.builder("rate.limit.pg.max", rateLimiter,
                            PaymentProviderRateLimiter::getRateLimit)
                    .description("PG rate limit max tokens")
                    .tag("provider", provider)
                    .register(meterRegistry);

            cachedPgCurrentTokens.put(provider, new AtomicLong(0));

            Gauge.builder("rate.limit.pg.current",
                            cachedPgCurrentTokens.get(provider),
                            AtomicLong::get)
                    .description("PG current available tokens")
                    .tag("provider", provider)
                    .register(meterRegistry);

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

    @Scheduled(fixedDelay = 5000, initialDelay = 2000)
    public void updateGlobalQueueSize() {
        // ORDER 큐
        globalQueueService.getQueueSize(QueueType.ORDER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> {
                            cachedGlobalOrderQueueSize.set(size);
                            log.info("Global ORDER queue: {}", size);
                        },
                        error -> log.warn("Failed to update global ORDER queue size: {}", error.getMessage())
                );

        // OTHER 큐
        globalQueueService.getQueueSize(QueueType.OTHER)
                .timeout(Duration.ofSeconds(3))
                .subscribe(
                        size -> {
                            cachedGlobalOtherQueueSize.set(size);
                            log.info("Global OTHER queue: {}", size);
                        },
                        error -> log.warn("Failed to update global OTHER queue size: {}", error.getMessage())
                );
    }

    @Scheduled(fixedDelay = 1000, initialDelay = 1000)
    public void updateGlobalWindowCount() {
        globalRateLimiterService.getCurrentWindowCount()
                .timeout(Duration.ofSeconds(2))
                .subscribe(
                        count -> {
                            cachedGlobalWindowCount.set(count);
                            int limit = globalRateLimiterService.getCurrentLimit();
                            double usage = limit > 0 ? (double) count / limit * 100 : 0;
                            log.info("Global: {}/{} ({}%)", count, limit, String.format("%.1f", usage));
                        },
                        error -> log.warn("Failed to update global window count: {}", error.getMessage())
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
                                log.info("PG {}: {}/{} used ({}%)", provider, used, max, String.format("%.1f", usage));
                            },
                            error -> log.warn("Failed to get PG {} tokens: {}", provider, error.getMessage())
                    );
        }
    }
}
