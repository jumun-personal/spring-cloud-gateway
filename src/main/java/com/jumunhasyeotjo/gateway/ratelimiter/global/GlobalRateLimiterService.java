package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Global Rate Limiter - Leaky Bucket 알고리즘
 *
 * [신규 요청] 토큰 획득 조건:
 * 1. 대기열 = 0
 * 2. 토큰 > 0
 *
 * [대기열 요청] 토큰 획득 조건:
 * 1. 토큰 > 0
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GlobalRateLimiterService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final LeakyBucketProperties leakyBucketProperties;
    private final ObjectMapper objectMapper;

    private static final String LEAKY_KEY = "leaky:global";
    private static final String ORDER_QUEUE_KEY = "queue:global:order";
    private static final String OTHER_QUEUE_KEY = "queue:global:other";

    // Feedback Loop용 limit 추적
    private final AtomicInteger previousLimit = new AtomicInteger(30);

    /**
     * 신규 요청용 Lua Script
     * 조건: 대기열 = 0 AND 토큰 > 0
     */
    private static final String NEW_REQUEST_LUA = """
        local leakyKey = KEYS[1]
        local orderQueueKey = KEYS[2]
        local otherQueueKey = KEYS[3]
        local now = tonumber(ARGV[1])
        local leakRate = tonumber(ARGV[2])
        local capacity = tonumber(ARGV[3])
        
        -- 1. 대기열 크기 확인
        local orderQueueSize = redis.call('ZCARD', orderQueueKey) or 0
        local otherQueueSize = redis.call('ZCARD', otherQueueKey) or 0
        local totalQueueSize = orderQueueSize + otherQueueSize
        
        -- 대기열에 사람 있으면 거부 (새치기 방지)
        if totalQueueSize > 0 then
            return cjson.encode({allowed = false, reason = 'QUEUE_NOT_EMPTY', queueSize = totalQueueSize})
        end
        
        -- 2. Leaky Bucket 체크
        local data = redis.call('GET', leakyKey)
        local water = 0
        local lastLeak = now
        
        if data then
            local parsed = cjson.decode(data)
            water = parsed.water or 0
            lastLeak = parsed.lastLeak or now
        end
        
        -- 경과 시간만큼 물 빼기
        local elapsedSec = (now - lastLeak) / 1000
        local leaked = math.floor(elapsedSec * leakRate)
        water = math.max(0, water - leaked)
        
        -- 버킷 가득 참?
        if water >= capacity then
            redis.call('SET', leakyKey, cjson.encode({water = water, lastLeak = now}), 'EX', 10)
            return cjson.encode({allowed = false, reason = 'BUCKET_FULL', water = water, capacity = capacity})
        end
        
        -- 허용
        water = water + 1
        redis.call('SET', leakyKey, cjson.encode({water = water, lastLeak = now}), 'EX', 10)
        
        return cjson.encode({allowed = true, water = water, capacity = capacity})
        """;

    /**
     * 대기열 요청용 Lua Script
     * 조건: 토큰 > 0
     */
    private static final String QUEUE_REQUEST_LUA = """
        local leakyKey = KEYS[1]
        local now = tonumber(ARGV[1])
        local leakRate = tonumber(ARGV[2])
        local capacity = tonumber(ARGV[3])
        
        local data = redis.call('GET', leakyKey)
        local water = 0
        local lastLeak = now
        
        if data then
            local parsed = cjson.decode(data)
            water = parsed.water or 0
            lastLeak = parsed.lastLeak or now
        end
        
        -- 경과 시간만큼 물 빼기
        local elapsedSec = (now - lastLeak) / 1000
        local leaked = math.floor(elapsedSec * leakRate)
        water = math.max(0, water - leaked)
        
        -- 버킷 가득?
        if water >= capacity then
            redis.call('SET', leakyKey, cjson.encode({water = water, lastLeak = now}), 'EX', 10)
            return cjson.encode({allowed = false, water = water, capacity = capacity})
        end
        
        -- 허용
        water = water + 1
        redis.call('SET', leakyKey, cjson.encode({water = water, lastLeak = now}), 'EX', 10)
        
        return cjson.encode({allowed = true, water = water, capacity = capacity})
        """;

    private RedisScript<String> newRequestScript;
    private RedisScript<String> queueRequestScript;

    @PostConstruct
    public void init() {
        newRequestScript = RedisScript.of(NEW_REQUEST_LUA, String.class);
        queueRequestScript = RedisScript.of(QUEUE_REQUEST_LUA, String.class);
    }

    /**
     * 신규 요청 토큰 획득 시도
     * 대기열 비어있고 토큰 있을 때만 허용
     */
    public Mono<Boolean> tryConsumeForNewRequest() {
        long now = System.currentTimeMillis();
        int rate = leakyBucketProperties.getGlobal().getRate();
        int capacity = leakyBucketProperties.getGlobal().getCapacity();

        return reactiveRedisTemplate.execute(
                newRequestScript,
                Arrays.asList(LEAKY_KEY, ORDER_QUEUE_KEY, OTHER_QUEUE_KEY),
                String.valueOf(now),
                String.valueOf(rate),
                String.valueOf(capacity)
        )
        .next()
        .map(result -> {
            try {
                Map<String, Object> parsed = objectMapper.readValue(result, Map.class);
                boolean allowed = (Boolean) parsed.get("allowed");
                if (!allowed) {
                    String reason = (String) parsed.get("reason");
                    log.debug("New request rejected: {}", reason);
                }
                return allowed;
            } catch (Exception e) {
                log.error("Failed to parse result", e);
                return false;
            }
        })
        .defaultIfEmpty(false)
        .onErrorReturn(false);
    }

    /**
     * 대기열 요청 토큰 획득 시도
     * 토큰만 있으면 허용
     */
    public Mono<Boolean> tryConsumeForQueueRequest() {
        long now = System.currentTimeMillis();
        int rate = leakyBucketProperties.getGlobal().getRate();
        int capacity = leakyBucketProperties.getGlobal().getCapacity();

        return reactiveRedisTemplate.execute(
                queueRequestScript,
                Collections.singletonList(LEAKY_KEY),
                String.valueOf(now),
                String.valueOf(rate),
                String.valueOf(capacity)
        )
        .next()
        .map(result -> {
            try {
                Map<String, Object> parsed = objectMapper.readValue(result, Map.class);
                return (Boolean) parsed.get("allowed");
            } catch (Exception e) {
                log.error("Failed to parse result", e);
                return false;
            }
        })
        .defaultIfEmpty(false)
        .onErrorReturn(false);
    }

    /**
     * @deprecated Use tryConsumeForNewRequest() or tryConsumeForQueueRequest()
     */
    @Deprecated
    public Mono<Boolean> tryConsume() {
        return tryConsumeForNewRequest();
    }

    public Mono<Long> getAvailableTokens() {
        return getCurrentWater()
                .map(water -> {
                    int capacity = leakyBucketProperties.getGlobal().getCapacity();
                    return Math.max(0L, capacity - water);
                });
    }

    public Mono<Long> getCurrentWater() {
        return reactiveRedisTemplate.opsForValue().get(LEAKY_KEY)
                .map(data -> {
                    try {
                        Map<String, Object> parsed = objectMapper.readValue(data, Map.class);
                        Number water = (Number) parsed.get("water");
                        return water != null ? water.longValue() : 0L;
                    } catch (Exception e) {
                        return 0L;
                    }
                })
                .defaultIfEmpty(0L);
    }

    public int getRate() {
        return leakyBucketProperties.getGlobal().getRate();
    }

    public int getCapacity() {
        return leakyBucketProperties.getGlobal().getCapacity();
    }

    // Feedback Loop용 메서드
    public int getPreviousLimit() {
        return previousLimit.get();
    }

    public void setPreviousLimit(int limit) {
        previousLimit.set(limit);
    }

    public Mono<Void> reset() {
        return reactiveRedisTemplate.delete(LEAKY_KEY)
                .doOnSuccess(deleted -> log.info("Leaky bucket reset"))
                .then();
    }

    public Mono<Boolean> isNearCapacity() {
        return getCurrentWater()
                .map(water -> {
                    int capacity = leakyBucketProperties.getGlobal().getCapacity();
                    double usage = (double) water / capacity;
                    return usage >= 0.9;
                })
                .onErrorReturn(false);
    }
}
