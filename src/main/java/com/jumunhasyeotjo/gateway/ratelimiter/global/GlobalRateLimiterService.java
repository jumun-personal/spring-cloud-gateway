package com.jumunhasyeotjo.gateway.ratelimiter.global;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Leaky Bucket 알고리즘 기반 글로벌 Rate Limiter
 *
 * 버킷에 물이 일정 속도(leakRate)로 빠져나가는 방식으로 동작:
 * - waterLevel: 현재 버킷에 쌓인 요청 수
 * - leakRate: 초당 처리(leak)되는 요청 수
 * - capacity: 버킷의 최대 용량
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GlobalRateLimiterService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    private static final String KEY = "leaky:global:bucket";
    private static final int TTL_SECONDS = 60;
    private static final int MIN_LIMIT = 10;
    private static final int MAX_LIMIT = 100;

    private final AtomicInteger leakRate = new AtomicInteger(15);
    private final AtomicInteger capacity = new AtomicInteger(15);

    // Leaky Bucket tryConsume Lua 스크립트
    private static final String TRY_CONSUME_SCRIPT = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local leakRate = tonumber(ARGV[2])
        local capacity = tonumber(ARGV[3])
        local ttl = tonumber(ARGV[4])

        local waterLevel = tonumber(redis.call('HGET', key, 'water_level') or '0')
        local lastLeakTime = tonumber(redis.call('HGET', key, 'last_leak_time') or tostring(now))

        -- 경과 시간에 따른 leak 계산
        local elapsedMs = now - lastLeakTime
        local elapsedSec = elapsedMs / 1000.0
        local leaked = leakRate * elapsedSec

        -- leak 적용 (물이 빠져나감)
        waterLevel = math.max(0, waterLevel - leaked)

        if waterLevel + 1 <= capacity then
            -- 요청 수락: 물 추가
            waterLevel = waterLevel + 1
            redis.call('HSET', key, 'water_level', tostring(waterLevel))
            redis.call('HSET', key, 'last_leak_time', tostring(now))
            redis.call('EXPIRE', key, ttl)
            return 1
        else
            -- 버킷 가득 참: 요청 거부 (상태는 업데이트)
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

    public Mono<Boolean> tryConsume() {
        long now = System.currentTimeMillis();

        return reactiveRedisTemplate.execute(
                tryConsumeScript,
                Collections.singletonList(KEY),
                String.valueOf(now),
                String.valueOf(leakRate.get()),
                String.valueOf(capacity.get()),
                String.valueOf(TTL_SECONDS)
        )
        .next()
        .map(result -> result == 1L)
        .defaultIfEmpty(false)
        .onErrorReturn(false);
    }

    public Mono<Long> getAvailableTokens() {
        return getCurrentWaterLevel()
                .map(waterLevel -> Math.max(0, capacity.get() - waterLevel))
                .onErrorReturn(0L);
    }

    public Mono<Long> getCurrentWindowCount() {
        return getCurrentWaterLevel();
    }

    private Mono<Long> getCurrentWaterLevel() {
        long now = System.currentTimeMillis();

        return reactiveRedisTemplate.execute(
                getWaterLevelScript,
                Collections.singletonList(KEY),
                String.valueOf(now),
                String.valueOf(leakRate.get())
        )
        .next()
        .map(result -> result / 1000)  // 소수점 복원 후 정수로
        .defaultIfEmpty(0L)
        .onErrorReturn(0L);
    }

    public void increaseLimit(int amount) {
        int current = leakRate.get();
        int newRate = Math.min(current + amount, MAX_LIMIT);
        leakRate.set(newRate);
        capacity.set(newRate);
        log.info("Leak rate increased: {} → {}", current, newRate);
    }

    public void decreaseLimit(int amount) {
        int current = leakRate.get();
        int newRate = Math.max(MIN_LIMIT, current - amount);
        leakRate.set(newRate);
        capacity.set(newRate);
        log.warn("Leak rate decreased: {} → {}", current, newRate);
    }

    public int getCurrentLimit() {
        return leakRate.get();
    }

    public Mono<Void> reset() {
        return reactiveRedisTemplate.delete(KEY)
                .doOnSuccess(deleted -> {
                    leakRate.set(30);
                    capacity.set(30);
                    log.info("Rate limiter reset");
                })
                .then();
    }

    public Mono<Boolean> isTokenSaturated() {
        return getCurrentWaterLevel()
                .map(level -> {
                    int cap = capacity.get();
                    double usage = (double) level / cap;
                    return usage >= 0.9;
                })
                .onErrorReturn(false);
    }
}
