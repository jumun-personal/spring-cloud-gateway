package com.jumunhasyeotjo.gateway.ratelimiter.global;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class GlobalRateLimiterService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    private static final String KEY = "sliding:global:requests";
    private static final long WINDOW_SIZE_MS = 1000;
    private final AtomicInteger currentLimit = new AtomicInteger(15);

    // Lua 스크립트: 원자적으로 count 확인 + 추가
    private static final String LUA_SCRIPT = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local windowStart = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])
        local requestId = ARGV[4]
        
        -- 오래된 요청 제거
        redis.call('ZREMRANGEBYSCORE', key, 0, windowStart)
        
        -- 현재 윈도우 카운트
        local count = redis.call('ZCARD', key)
        
        if count >= limit then
            return 0
        end
        
        -- 요청 추가
        redis.call('ZADD', key, now, requestId)
        redis.call('EXPIRE', key, 2)
        
        return 1
        """;

    private RedisScript<Long> rateLimitScript;

    @PostConstruct
    public void init() {
        rateLimitScript = RedisScript.of(LUA_SCRIPT, Long.class);
    }

    public Mono<Boolean> tryConsume() {
        long now = System.currentTimeMillis();
        long windowStart = now - WINDOW_SIZE_MS;
        int limit = currentLimit.get();
        String requestId = now + ":" + Thread.currentThread().getId() + ":" + System.nanoTime();

        return reactiveRedisTemplate.execute(
                rateLimitScript,
                Collections.singletonList(KEY),
                String.valueOf(now),
                String.valueOf(windowStart),
                String.valueOf(limit),
                requestId
        )
        .next()
        .map(result -> result == 1L)
        .defaultIfEmpty(false)
        .onErrorReturn(false);
    }

    public Mono<Long> getAvailableTokens() {
        long now = System.currentTimeMillis();
        long windowStart = now - WINDOW_SIZE_MS;
        int limit = currentLimit.get();

        return countWindowRequests(windowStart, now)
                .map(count -> Math.max(0, limit - count))
                .onErrorReturn(0L);
    }

    public Mono<Long> getCurrentWindowCount() {
        long now = System.currentTimeMillis();
        long windowStart = now - WINDOW_SIZE_MS;
        return countWindowRequests(windowStart, now).onErrorReturn(0L);
    }

    private Mono<Long> countWindowRequests(long windowStart, long windowEnd) {
        Range<Double> range = Range.closed((double) windowStart, (double) windowEnd);
        return reactiveRedisTemplate.opsForZSet().count(KEY, range);
    }

    public void increaseLimit(int amount) {
        int current = currentLimit.get();
        int newLimit = current + amount;
        currentLimit.set(newLimit);
        log.info("Rate limit increased: {} → {}", current, newLimit);
    }

    public void decreaseLimit(int amount) {
        int current = currentLimit.get();
        int newLimit = Math.max(10, current - amount);
        currentLimit.set(newLimit);
        log.warn("Rate limit decreased: {} → {}", current, newLimit);
    }

    public int getCurrentLimit() {
        return currentLimit.get();
    }

    public Mono<Void> reset() {
        return reactiveRedisTemplate.delete(KEY)
                .doOnSuccess(deleted -> {
                    currentLimit.set(30);
                    log.info("Rate limiter reset");
                })
                .then();
    }

    public Mono<Boolean> isTokenSaturated() {
        return getCurrentWindowCount()
                .map(count -> {
                    int max = getCurrentLimit();
                    double usage = (double) count / max;
                    return usage >= 0.9;
                })
                .onErrorReturn(false);
    }
}
