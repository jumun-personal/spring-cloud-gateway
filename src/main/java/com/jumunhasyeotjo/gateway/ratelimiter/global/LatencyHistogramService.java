package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

/**
 * Redis Time-Sliced Histogram을 이용한 P95/P99 근사치 계산
 *
 * 구간별 카운트를 누적하여 백분위수 계산
 * 구간: 0-50ms, 50-100ms, 100-200ms, 200-500ms, 500-1000ms, 1000-2000ms, 2000ms+
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LatencyHistogramService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final ObjectMapper objectMapper;

    private static final String HISTOGRAM_KEY = "latency:histogram:gateway";
    private static final Duration HISTOGRAM_TTL = Duration.ofMinutes(5);

    // 구간 경계 (ms)
    private static final int[] BUCKETS = {50, 100, 200, 500, 1000, 2000, Integer.MAX_VALUE};
    private static final String[] BUCKET_NAMES = {"b50", "b100", "b200", "b500", "b1000", "b2000", "bInf"};

    /**
     * Latency 기록 Lua Script
     */
    private static final String RECORD_LATENCY_LUA = """
        local key = KEYS[1]
        local latencyMs = tonumber(ARGV[1])
        local ttl = tonumber(ARGV[2])
        
        -- 구간 결정
        local buckets = {50, 100, 200, 500, 1000, 2000}
        local bucketNames = {'b50', 'b100', 'b200', 'b500', 'b1000', 'b2000', 'bInf'}
        local bucketIndex = 7  -- default: bInf
        
        for i, boundary in ipairs(buckets) do
            if latencyMs <= boundary then
                bucketIndex = i
                break
            end
        end
        
        local bucketName = bucketNames[bucketIndex]
        
        -- 해당 구간 카운트 증가
        redis.call('HINCRBY', key, bucketName, 1)
        redis.call('HINCRBY', key, 'total', 1)
        redis.call('EXPIRE', key, ttl)
        
        return bucketName
        """;

    /**
     * P95/P99 계산 Lua Script
     */
    private static final String CALCULATE_PERCENTILE_LUA = """
        local key = KEYS[1]
        
        local data = redis.call('HGETALL', key)
        if #data == 0 then
            return cjson.encode({p95 = 0, p99 = 0, total = 0})
        end
        
        -- Hash를 테이블로 변환
        local counts = {}
        local total = 0
        for i = 1, #data, 2 do
            local field = data[i]
            local value = tonumber(data[i + 1]) or 0
            counts[field] = value
            if field == 'total' then
                total = value
            end
        end
        
        if total == 0 then
            return cjson.encode({p95 = 0, p99 = 0, total = 0})
        end
        
        -- 구간별 중앙값 (근사치용)
        local bucketMidpoints = {25, 75, 150, 350, 750, 1500, 3000}
        local bucketNames = {'b50', 'b100', 'b200', 'b500', 'b1000', 'b2000', 'bInf'}
        
        -- P95, P99 위치
        local p95Pos = math.ceil(total * 0.95)
        local p99Pos = math.ceil(total * 0.99)
        
        local cumulative = 0
        local p95 = 0
        local p99 = 0
        
        for i, name in ipairs(bucketNames) do
            local count = counts[name] or 0
            cumulative = cumulative + count
            
            if p95 == 0 and cumulative >= p95Pos then
                p95 = bucketMidpoints[i]
            end
            if p99 == 0 and cumulative >= p99Pos then
                p99 = bucketMidpoints[i]
            end
        end
        
        return cjson.encode({p95 = p95, p99 = p99, total = total})
        """;

    private RedisScript<String> recordLatencyScript;
    private RedisScript<String> calculatePercentileScript;

    @PostConstruct
    public void init() {
        recordLatencyScript = RedisScript.of(RECORD_LATENCY_LUA, String.class);
        calculatePercentileScript = RedisScript.of(CALCULATE_PERCENTILE_LUA, String.class);
    }

    /**
     * Latency 기록
     */
    public Mono<Void> recordLatency(long latencyMs) {
        return reactiveRedisTemplate.execute(
                recordLatencyScript,
                Collections.singletonList(HISTOGRAM_KEY),
                String.valueOf(latencyMs),
                String.valueOf(HISTOGRAM_TTL.toSeconds())
        )
        .then()
        .doOnError(e -> log.error("Failed to record latency", e))
        .onErrorResume(e -> Mono.empty());
    }

    /**
     * P95/P99 근사치 계산
     */
    public Mono<PercentileResult> getPercentiles() {
        return reactiveRedisTemplate.execute(
                calculatePercentileScript,
                Collections.singletonList(HISTOGRAM_KEY)
        )
        .next()
        .map(result -> {
            try {
                Map<String, Object> parsed = objectMapper.readValue(result, Map.class);
                return new PercentileResult(
                        ((Number) parsed.get("p95")).doubleValue(),
                        ((Number) parsed.get("p99")).doubleValue(),
                        ((Number) parsed.get("total")).longValue()
                );
            } catch (Exception e) {
                log.error("Failed to parse percentile result", e);
                return new PercentileResult(0, 0, 0);
            }
        })
        .defaultIfEmpty(new PercentileResult(0, 0, 0));
    }

    /**
     * Histogram 초기화
     */
    public Mono<Void> reset() {
        return reactiveRedisTemplate.delete(HISTOGRAM_KEY)
                .doOnSuccess(deleted -> log.info("Latency histogram reset"))
                .then();
    }

    @Data
    public static class PercentileResult {
        private final double p95;
        private final double p99;
        private final long total;

        public PercentileResult(double p95, double p99, long total) {
            this.p95 = p95;
            this.p99 = p99;
            this.total = total;
        }
    }
}
