package com.jumunhasyeotjo.gateway.ratelimiter.pg.toss;

import com.jumunhasyeotjo.gateway.ratelimiter.global.LeakyBucketProperties;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.PaymentProviderRateLimiter;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Toss PG Rate Limiter - Leaky Bucket
 * 
 * capacity = rate로 설정하여 버스트 불허
 */
@Component
@RequiredArgsConstructor
public class TossPaymentRateLimiter implements PaymentProviderRateLimiter {

    private final ProxyManager<String> proxyManager;
    private final LeakyBucketProperties leakyBucketProperties;
    
    private static final String BUCKET_KEY = "bucket:toss:leaky";

    @Override
    public Mono<Boolean> tryConsume() {
        return Mono.fromCallable(() -> {
            Bucket bucket = getBucket();
            return bucket.tryConsume(1);
        });
    }

    @Override
    public String getProviderName() {
        return "TOSS";
    }

    @Override
    public int getRateLimit() {
        return leakyBucketProperties.getPg().getToss().getRate();
    }

    @Override
    public Mono<Long> getAvailableTokens() {
        return Mono.fromCallable(() -> getBucket().getAvailableTokens());
    }

    private Bucket getBucket() {
        int rate = leakyBucketProperties.getPg().getToss().getRate();
        int capacity = leakyBucketProperties.getPg().getToss().getCapacity();
        
        // Leaky Bucket: capacity = rate (버스트 불허)
        BucketConfiguration config = BucketConfiguration.builder()
                .addLimit(Bandwidth.builder()
                        .capacity(capacity)
                        .refillGreedy(rate, Duration.ofSeconds(1))
                        .build())
                .build();
        return proxyManager.builder().build(BUCKET_KEY, () -> config);
    }
}
