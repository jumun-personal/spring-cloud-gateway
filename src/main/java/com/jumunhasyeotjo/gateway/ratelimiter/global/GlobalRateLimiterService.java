package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class GlobalRateLimiterService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final ApplicationEventPublisher eventPublisher;

    private static final String KEY = "sliding:global:requests";
    private static final long WINDOW_SIZE_MS = 1000;
    private final AtomicInteger currentLimit = new AtomicInteger(30);

    public Mono<Boolean> tryConsume() {
        long now = System.currentTimeMillis();
        long windowStart = now - WINDOW_SIZE_MS;
        int limit = currentLimit.get();

        return cleanOldRequests(windowStart)
                .then(countWindowRequests(windowStart, now))
                .flatMap(count -> {
                    if (count >= limit) {
                        log.debug("Rate limit exceeded: {}/{}", count, limit);
                        return Mono.just(false);
                    }

                    return recordNewRequest(now)
                            .doOnSuccess(v -> eventPublisher.publishEvent(
                                    new GlobalTokenRefilledEvent(this)))
                            .thenReturn(true);
                })
                .onErrorReturn(false);
    }

    public Mono<Long> getCurrentWindowCount() {
        long now = System.currentTimeMillis();
        long windowStart = now - WINDOW_SIZE_MS;

        return countWindowRequests(windowStart, now)
                .onErrorReturn(0L);
    }

    /**
     * 윈도우 밖의 오래된 요청 제거
     */
    private Mono<Long> cleanOldRequests(long windowStart) {
        Range<Double> oldRange = Range.closed(0.0, (double) windowStart);

        return reactiveRedisTemplate.opsForZSet()
                .removeRangeByScore(KEY, oldRange)
                .doOnNext(removed -> {
                    if (removed > 0) {
                        log.debug("Removed {} old requests", removed);
                    }
                });
    }

    /**
     * 현재 윈도우 내 요청 수 카운트
     */
    private Mono<Long> countWindowRequests(long windowStart, long windowEnd) {
        Range<Double> range = Range.closed((double) windowStart, (double) windowEnd);

        return reactiveRedisTemplate.opsForZSet()
                .count(KEY, range);
    }

    /**
     * 새 요청 기록
     */
    private Mono<Boolean> recordNewRequest(long timestamp) {
        String requestId = timestamp + ":" + Thread.currentThread().getId();

        return reactiveRedisTemplate.opsForZSet()
                .add(KEY, requestId, timestamp)
                .then(reactiveRedisTemplate.expire(KEY, Duration.ofSeconds(2)))
                .thenReturn(true);
    }

    public void increaseLimit(int amount) {
        int current = currentLimit.get();
        int newLimit = current + amount;
        currentLimit.set(newLimit);

        log.info(" Rate limit increased: {} → {} (+{})", current, newLimit, amount);
    }

    /**
     * Rate Limit 감소 (amount만큼, 최대 30)
     */
    public void decreaseLimit(int amount) {
        int current = currentLimit.get();
        int newLimit = Math.max(10, current - amount);  // 최소 10
        currentLimit.set(newLimit);

        log.warn(" Rate limit decreased: {} → {} (-{})", current, newLimit, amount);
    }

    public int getCurrentLimit() {
        return currentLimit.get();
    }

    public Mono<Void> reset() {
        return reactiveRedisTemplate.delete(KEY)
                .doOnSuccess(deleted -> {
                    currentLimit.set(30);
                    log.info(" Rate limiter reset");
                })
                .then();
    }

    public Mono<Boolean> isTokenSaturated() {
        return getCurrentWindowCount()
                .map(count -> {
                    int max = getCurrentLimit();
                    double usage = (double) count / max;

                    boolean saturated = usage >= 0.9;
                    log.debug("Token saturation: {}/{} ({:.1f}%)",
                            count, max, usage * 100);

                    return saturated;
                })
                .onErrorReturn(false);  // 에러 시 false 반환
    }
}