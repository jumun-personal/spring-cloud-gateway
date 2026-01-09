package com.jumunhasyeotjo.gateway.ratelimiter.pg.toss;


import com.jumunhasyeotjo.gateway.ratelimiter.pg.PaymentProviderRateLimiter;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.TokenRefilledEvent;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class TossPaymentRateLimiter implements PaymentProviderRateLimiter {

    private final ProxyManager<String> proxyManager;
    private final ApplicationEventPublisher eventPublisher;
    private static final String BUCKET_KEY = "bucket:toss";
    private static final int RATE_LIMIT = 30;

    @Override
    public Mono<Boolean> tryConsume() {
        return Mono.fromCallable(() -> {
            BucketConfiguration configuration = BucketConfiguration.builder()
                    .addLimit(limit -> limit.capacity(RATE_LIMIT).refillGreedy(RATE_LIMIT, Duration.ofSeconds(1)))
                    .build();

            Bucket bucket = proxyManager.getProxy(BUCKET_KEY, () -> configuration);

            boolean consumed = bucket.tryConsume(1);

            if (consumed) {
                eventPublisher.publishEvent(new TokenRefilledEvent(this));
            }

            return consumed;
        });
    }

    @Override
    public String getProviderName() {
        return "TOSS";
    }

    @Override
    public int getRateLimit() {
        return RATE_LIMIT;
    }

    @Override
    public Mono<Long> getAvailableTokens() {
        return Mono.fromCallable(() -> {
            Bucket bucket = getBucket();
            return bucket.getAvailableTokens();
        });
    }

    private Bucket getBucket() {
        BucketConfiguration config = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(RATE_LIMIT, Duration.ofSeconds(1)))
                .build();

        return proxyManager.builder()
                .build(BUCKET_KEY, () -> config);
    }
}
