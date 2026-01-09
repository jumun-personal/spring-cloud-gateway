package com.jumunhasyeotjo.gateway.ratelimiter.global;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class GlobalRateLimiterService {

    private final ProxyManager<String> proxyManager;
    private final ApplicationEventPublisher eventPublisher;
    private static final String BUCKET_KEY = "bucket:global:sliding-window";
    private final AtomicInteger currentLimit = new AtomicInteger(30);

    public Mono<Boolean> tryConsume() {
        return Mono.fromCallable(() -> {
            Bucket bucket = getBucket();
            boolean consumed = bucket.tryConsume(1);

            if (consumed) {
                eventPublisher.publishEvent(new GlobalTokenRefilledEvent(this));
            }

            log.debug("Rate limit check - consumed: {}, available: {}/{}",
                    consumed, bucket.getAvailableTokens(), currentLimit.get());

            return consumed;
        });
    }

    public Mono<Long> getCurrentWindowCount() {
        return Mono.fromCallable(() -> {
            Bucket bucket = getBucket();
            long available = bucket.getAvailableTokens();
            long total = currentLimit.get();
            return total - available;
        });
    }

    private Bucket getBucket() {
        int limit = currentLimit.get();

        BucketConfiguration config = BucketConfiguration.builder()
                .addLimit(Bandwidth.builder()
                        .capacity(limit)
                        .refillIntervally(limit, Duration.ofSeconds(1))
                        .build()
                )
                .build();

        return proxyManager.builder()
                .build(BUCKET_KEY, () -> config);
    }

    public void increaseLimit() {
        int current = currentLimit.get();
        int newLimit = current + 1;
        currentLimit.set(newLimit);

        log.info("Sliding Window rate limit increased: {} -> {}", current, newLimit);

        // 버킷 설정 갱신
        refreshBucket();
    }

    public void decreaseLimit(int amount) {
        int current = currentLimit.get();
        int newLimit = Math.max(10, current - amount);
        currentLimit.set(newLimit);

        log.warn("Sliding Window rate limit decreased: {} -> {}", current, newLimit);

        // 버킷 설정 갱신
        refreshBucket();
    }

    private void refreshBucket() {
        // ProxyManager를 통해 기존 버킷 제거
        try {
            proxyManager.removeProxy(BUCKET_KEY);
            log.debug("Bucket configuration refreshed for key: {}", BUCKET_KEY);
        } catch (Exception e) {
            log.warn("Failed to remove old bucket, will be overwritten", e);
        }
    }

    public int getCurrentLimit() {
        return currentLimit.get();
    }

    public Mono<Void> reset() {
        return Mono.fromRunnable(() -> {
            proxyManager.removeProxy(BUCKET_KEY);
            currentLimit.set(30);
            log.info("Rate limiter reset to default: 30 req/sec");
        });
    }
}