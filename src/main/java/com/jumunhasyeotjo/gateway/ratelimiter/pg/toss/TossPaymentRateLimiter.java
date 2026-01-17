package com.jumunhasyeotjo.gateway.ratelimiter.pg.toss;

import com.jumunhasyeotjo.gateway.ratelimiter.pg.PaymentProviderRateLimiter;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.Collections;

/**
 * Leaky Bucket 알고리즘 기반 Toss PG Rate Limiter
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TossPaymentRateLimiter implements PaymentProviderRateLimiter {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    private static final String BUCKET_KEY = "leaky:pg:toss";
    private static final int RATE_LIMIT = 10;
    private static final int CAPACITY = 10;
    private static final int TTL_SECONDS = 60;

    // Leaky Bucket tryConsume Lua 스크립트
    private static final String TRY_CONSUME_SCRIPT = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local leakRate = tonumber(ARGV[2])
        local capacity = tonumber(ARGV[3])
        local ttl = tonumber(ARGV[4])

        local waterLevel = tonumber(redis.call('HGET', key, 'water_level') or '0')
        local lastLeakTime = tonumber(redis.call('HGET', key, 'last_leak_time') or tostring(now))

        local elapsedMs = now - lastLeakTime
        local elapsedSec = elapsedMs / 1000.0
        local leaked = leakRate * elapsedSec

        waterLevel = math.max(0, waterLevel - leaked)

        if waterLevel + 1 <= capacity then
            waterLevel = waterLevel + 1
            redis.call('HSET', key, 'water_level', tostring(waterLevel))
            redis.call('HSET', key, 'last_leak_time', tostring(now))
            redis.call('EXPIRE', key, ttl)
            return 1
        else
            redis.call('HSET', key, 'water_level', tostring(waterLevel))
            redis.call('HSET', key, 'last_leak_time', tostring(now))
            redis.call('EXPIRE', key, ttl)
            return 0
        end
        """;

    // 현재 water level 조회 Lua 스크립트
    private static final String GET_WATER_LEVEL_SCRIPT = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local leakRate = tonumber(ARGV[2])

        local waterLevel = tonumber(redis.call('HGET', key, 'water_level') or '0')
        local lastLeakTime = tonumber(redis.call('HGET', key, 'last_leak_time') or tostring(now))

        local elapsedMs = now - lastLeakTime
        local elapsedSec = elapsedMs / 1000.0
        local leaked = leakRate * elapsedSec

        waterLevel = math.max(0, waterLevel - leaked)

        return math.floor(waterLevel * 1000)
        """;

    private RedisScript<Long> tryConsumeScript;
    private RedisScript<Long> getWaterLevelScript;

    @PostConstruct
    public void init() {
        tryConsumeScript = RedisScript.of(TRY_CONSUME_SCRIPT, Long.class);
        getWaterLevelScript = RedisScript.of(GET_WATER_LEVEL_SCRIPT, Long.class);
    }

    @Override
    public Mono<Boolean> tryConsume() {
        long now = System.currentTimeMillis();

        return reactiveRedisTemplate.execute(
                tryConsumeScript,
                Collections.singletonList(BUCKET_KEY),
                String.valueOf(now),
                String.valueOf(RATE_LIMIT),
                String.valueOf(CAPACITY),
                String.valueOf(TTL_SECONDS)
        )
        .next()
        .map(result -> result == 1L)
        .defaultIfEmpty(false)
        .onErrorReturn(false);
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
        long now = System.currentTimeMillis();

        return reactiveRedisTemplate.execute(
                getWaterLevelScript,
                Collections.singletonList(BUCKET_KEY),
                String.valueOf(now),
                String.valueOf(RATE_LIMIT)
        )
        .next()
        .map(result -> {
            long waterLevel = result / 1000;
            return Math.max(0, CAPACITY - waterLevel);
        })
        .defaultIfEmpty((long) CAPACITY)
        .onErrorReturn((long) CAPACITY);
    }
}
