package com.jumunhasyeotjo.gateway.ratelimiter.global;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    private static final String ORDER_QUEUE_KEY = "queue:global:order";
    private static final String OTHER_QUEUE_KEY = "queue:global:other";
    private static final String ORDER_RETRY_QUEUE_KEY = "queue:global:order:retry";
    private static final String OTHER_RETRY_QUEUE_KEY = "queue:global:other:retry";
    private static final int TTL_SECONDS = 60;
    private static final int MIN_LIMIT = 10;
    private static final int MAX_LIMIT = 100;

    private final AtomicInteger leakRate = new AtomicInteger(15);
    private final AtomicInteger capacity = new AtomicInteger(15);

    // Leaky Bucket tryConsume Lua 스크립트 (큐 인식 버전)
    // KEYS[1]=버킷키, KEYS[2]=ORDER큐, KEYS[3]=OTHER큐, KEYS[4]=ORDER재시도큐, KEYS[5]=OTHER재시도큐
    // ARGV[1]=now, ARGV[2]=leakRate, ARGV[3]=capacity, ARGV[4]=ttl, ARGV[5]=isNewRequest
    // 반환값: 1=허용, 0=용량부족, -1=대기열비어있지않음
    private static final String TRY_CONSUME_SCRIPT = """
        local bucketKey = KEYS[1]
        local orderQueueKey = KEYS[2]
        local otherQueueKey = KEYS[3]
        local orderRetryQueueKey = KEYS[4]
        local otherRetryQueueKey = KEYS[5]

        local now = tonumber(ARGV[1])
        local leakRate = tonumber(ARGV[2])
        local capacity = tonumber(ARGV[3])
        local ttl = tonumber(ARGV[4])
        local isNewRequest = tonumber(ARGV[5])

        local waterLevel = tonumber(redis.call('HGET', bucketKey, 'water_level') or '0')
        local lastLeakTime = tonumber(redis.call('HGET', bucketKey, 'last_leak_time') or tostring(now))

        -- 경과 시간에 따른 leak 계산
        local elapsedMs = now - lastLeakTime
        local elapsedSec = elapsedMs / 1000.0
        local leaked = leakRate * elapsedSec

        -- leak 적용 (물이 빠져나감)
        waterLevel = math.max(0, waterLevel - leaked)

        -- 신규 요청인 경우: 대기열 비어있는지 확인
        if isNewRequest == 1 then
            local totalQueueSize = redis.call('ZCARD', orderQueueKey)
                                 + redis.call('ZCARD', otherQueueKey)
                                 + redis.call('ZCARD', orderRetryQueueKey)
                                 + redis.call('ZCARD', otherRetryQueueKey)

            if totalQueueSize > 0 then
                -- 대기열에 요청이 있음: 신규 요청 거부 (공정성 유지)
                redis.call('HSET', bucketKey, 'water_level', tostring(waterLevel))
                redis.call('HSET', bucketKey, 'last_leak_time', tostring(now))
                redis.call('EXPIRE', bucketKey, ttl)
                return -1
            end
        end

        if waterLevel + 1 <= capacity then
            -- 요청 수락: 물 추가
            waterLevel = waterLevel + 1
            redis.call('HSET', bucketKey, 'water_level', tostring(waterLevel))
            redis.call('HSET', bucketKey, 'last_leak_time', tostring(now))
            redis.call('EXPIRE', bucketKey, ttl)
            return 1
        else
            -- 버킷 가득 참: 요청 거부 (상태는 업데이트)
            redis.call('HSET', bucketKey, 'water_level', tostring(waterLevel))
            redis.call('HSET', bucketKey, 'last_leak_time', tostring(now))
            redis.call('EXPIRE', bucketKey, ttl)
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

    /**
     * 토큰 소비 시도 (하위 호환성 유지 - 신규 요청으로 처리)
     */
    public Mono<Boolean> tryConsume() {
        return tryConsume(true)
                .map(result -> result == TryConsumeResult.ALLOWED);
    }

    /**
     * 토큰 소비 시도 (큐 인식 버전)
     *
     * @param isNewRequest true: 신규 요청 (대기열 비어있어야 토큰 획득 가능)
     *                     false: 대기열 요청 (토큰만 있으면 획득 가능)
     * @return TryConsumeResult 결과
     */
    public Mono<TryConsumeResult> tryConsume(boolean isNewRequest) {
        long now = System.currentTimeMillis();

        List<String> keys = Arrays.asList(
                KEY,
                ORDER_QUEUE_KEY,
                OTHER_QUEUE_KEY,
                ORDER_RETRY_QUEUE_KEY,
                OTHER_RETRY_QUEUE_KEY
        );

        return reactiveRedisTemplate.execute(
                tryConsumeScript,
                keys,
                String.valueOf(now),
                String.valueOf(leakRate.get()),
                String.valueOf(capacity.get()),
                String.valueOf(TTL_SECONDS),
                String.valueOf(isNewRequest ? 1 : 0)
        )
        .next()
        .map(result -> TryConsumeResult.fromCode(result.intValue()))
        .defaultIfEmpty(TryConsumeResult.ERROR)
        .onErrorReturn(TryConsumeResult.ERROR);
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

    /**
     * floor 제약 조건이 있는 limit 설정 (Scale-Out 피드백 루프용)
     * limit은 절대 floor 아래로 내려가지 않음
     *
     * @param newLimit 새로운 limit 값
     * @param floor 최소 limit (Scale-Out 시 저장된 이전 limit)
     */
    public void setLimitWithFloor(int newLimit, int floor) {
        int safeLimit = Math.max(newLimit, floor);
        safeLimit = Math.min(safeLimit, MAX_LIMIT);
        safeLimit = Math.max(safeLimit, MIN_LIMIT);

        int current = leakRate.get();
        leakRate.set(safeLimit);
        capacity.set(safeLimit);
        log.info("Limit set with floor: {} → {} (floor={})", current, safeLimit, floor);
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

    /**
     * tryConsume 결과를 나타내는 열거형
     */
    @Getter
    public enum TryConsumeResult {
        ALLOWED(1),           // 토큰 획득 성공
        DENIED_CAPACITY(0),   // 버킷 용량 부족으로 거부
        DENIED_QUEUE(-1),     // 대기열이 비어있지 않아 거부 (신규 요청만)
        ERROR(-999);          // Redis 오류

        private final int code;

        TryConsumeResult(int code) {
            this.code = code;
        }

        public static TryConsumeResult fromCode(int code) {
            return switch (code) {
                case 1 -> ALLOWED;
                case 0 -> DENIED_CAPACITY;
                case -1 -> DENIED_QUEUE;
                default -> ERROR;
            };
        }
    }
}
