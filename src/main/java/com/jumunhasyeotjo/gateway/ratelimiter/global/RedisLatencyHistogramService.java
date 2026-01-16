package com.jumunhasyeotjo.gateway.ratelimiter.global;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;

/**
 * Redis 기반 시간 분할 히스토그램 서비스
 *
 * - 레이턴시 측정값을 Redis에 누적 합(cumulative sum)으로 저장
 * - P95/P99 백분위수를 선형 보간법으로 계산
 * - 시간 슬라이스 단위로 데이터 관리 (기본 10초 단위, 60초 데이터 유지)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisLatencyHistogramService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final FeedbackLoopProperties properties;

    private static final String HISTOGRAM_KEY_PREFIX = "latency:histogram:";

    // Lua script for recording latency (cumulative histogram)
    // Increments all buckets >= latency value
    private static final String RECORD_LATENCY_SCRIPT = """
        local histogramKey = KEYS[1]
        local latencyMs = tonumber(ARGV[1])
        local ttlSeconds = tonumber(ARGV[2])
        local bucketCount = tonumber(ARGV[3])

        -- Bucket boundaries are passed as ARGV[4] onwards
        for i = 1, bucketCount do
            local boundary = tonumber(ARGV[3 + i])
            if latencyMs <= boundary then
                -- Increment all buckets from this one to the highest
                for j = i, bucketCount do
                    local bucketField = ARGV[3 + j]
                    redis.call('HINCRBY', histogramKey, bucketField, 1)
                end
                break
            end
        end

        -- If latency exceeds all buckets, increment only the highest bucket
        local highestBucket = ARGV[3 + bucketCount]
        local currentCount = tonumber(redis.call('HGET', histogramKey, highestBucket) or '0')
        if latencyMs > tonumber(highestBucket) then
            redis.call('HINCRBY', histogramKey, highestBucket, 1)
        end

        redis.call('EXPIRE', histogramKey, ttlSeconds)
        return 1
        """;

    private RedisScript<Long> recordLatencyScript;

    @PostConstruct
    public void init() {
        recordLatencyScript = RedisScript.of(RECORD_LATENCY_SCRIPT, Long.class);
    }

    /**
     * 레이턴시 측정값 기록
     * 해당 레이턴시에 맞는 버킷부터 최대 버킷까지 모든 값을 증가 (누적 히스토그램)
     */
    public Mono<Void> recordLatency(long latencyMs) {
        var histogramParams = properties.getHistogram();
        long timeSlice = getTimeSlice(histogramParams.getTimeSliceDurationMs());
        String histogramKey = HISTOGRAM_KEY_PREFIX + timeSlice;

        int[] boundaries = histogramParams.getBucketBoundaries();
        int ttlSeconds = (histogramParams.getTimeSliceDurationMs() * histogramParams.getMaxSlices()) / 1000 + 10;

        // Build arguments: latencyMs, ttl, bucketCount, boundary1, boundary2, ...
        List<String> args = new ArrayList<>();
        args.add(String.valueOf(latencyMs));
        args.add(String.valueOf(ttlSeconds));
        args.add(String.valueOf(boundaries.length));
        for (int boundary : boundaries) {
            args.add(String.valueOf(boundary));
        }

        return reactiveRedisTemplate.execute(
                        recordLatencyScript,
                        Collections.singletonList(histogramKey),
                        args.toArray(new String[0])
                )
                .then()
                .doOnError(error -> log.warn("Failed to record latency: {}", error.getMessage()));
    }

    /**
     * P95 레이턴시 계산 (밀리초)
     */
    public Mono<Double> getP95Latency() {
        return calculatePercentile(0.95);
    }

    /**
     * P99 레이턴시 계산 (밀리초)
     */
    public Mono<Double> getP99Latency() {
        return calculatePercentile(0.99);
    }

    /**
     * 백분위수 계산 (선형 보간법 사용)
     *
     * 알고리즘:
     * 1. 최근 시간 슬라이스의 히스토그램 데이터 집계
     * 2. 총 카운트 산출
     * 3. 목표 카운트 = 총 카운트 * percentile
     * 4. 누적 카운트가 목표를 넘는 버킷 구간 찾기
     * 5. 선형 보간으로 정확한 값 계산
     */
    private Mono<Double> calculatePercentile(double percentile) {
        return aggregateRecentHistograms()
                .map(histogram -> {
                    if (histogram.isEmpty()) {
                        log.debug("No histogram data available for P{}", (int) (percentile * 100));
                        return 0.0;
                    }

                    int[] boundaries = properties.getHistogram().getBucketBoundaries();

                    // Get total count from highest bucket (cumulative)
                    long totalCount = histogram.getOrDefault(boundaries[boundaries.length - 1], 0L);

                    if (totalCount == 0) {
                        return 0.0;
                    }

                    long targetCount = (long) Math.ceil(totalCount * percentile);

                    // Find the bucket where cumulative count >= targetCount
                    long prevCount = 0;
                    int prevBoundary = 0;

                    for (int boundary : boundaries) {
                        long cumulativeCount = histogram.getOrDefault(boundary, 0L);

                        if (cumulativeCount >= targetCount) {
                            // Linear interpolation
                            if (cumulativeCount == prevCount) {
                                return (double) boundary;
                            }

                            double fraction = (double) (targetCount - prevCount) /
                                    (cumulativeCount - prevCount);
                            return prevBoundary + fraction * (boundary - prevBoundary);
                        }

                        prevCount = cumulativeCount;
                        prevBoundary = boundary;
                    }

                    // All values are above highest bucket
                    return (double) boundaries[boundaries.length - 1];
                });
    }

    /**
     * 최근 시간 슬라이스의 히스토그램 데이터 집계
     */
    private Mono<Map<Integer, Long>> aggregateRecentHistograms() {
        var histogramParams = properties.getHistogram();
        int timeSliceDurationMs = histogramParams.getTimeSliceDurationMs();
        int maxSlices = histogramParams.getMaxSlices();

        long currentSlice = getTimeSlice(timeSliceDurationMs);

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < maxSlices; i++) {
            keys.add(HISTOGRAM_KEY_PREFIX + (currentSlice - (long) i * timeSliceDurationMs));
        }

        return Flux.fromIterable(keys)
                .flatMap(key -> reactiveRedisTemplate.opsForHash().entries(key)
                        .collectMap(
                                entry -> Integer.parseInt(entry.getKey().toString()),
                                entry -> Long.parseLong(entry.getValue().toString())
                        )
                        .onErrorReturn(Collections.emptyMap())
                )
                .collectList()
                .map(this::mergeHistograms);
    }

    /**
     * 여러 시간 슬라이스의 히스토그램 병합
     */
    private Map<Integer, Long> mergeHistograms(List<Map<Integer, Long>> histogramList) {
        Map<Integer, Long> merged = new HashMap<>();

        for (Map<Integer, Long> histogram : histogramList) {
            for (Map.Entry<Integer, Long> entry : histogram.entrySet()) {
                merged.merge(entry.getKey(), entry.getValue(), Long::sum);
            }
        }

        return merged;
    }

    /**
     * 현재 시간 슬라이스 계산
     */
    private long getTimeSlice(int durationMs) {
        return (System.currentTimeMillis() / durationMs) * durationMs;
    }

    /**
     * 히스토그램 데이터 삭제 (테스트용)
     */
    public Mono<Void> clearHistograms() {
        var histogramParams = properties.getHistogram();
        int timeSliceDurationMs = histogramParams.getTimeSliceDurationMs();
        int maxSlices = histogramParams.getMaxSlices();
        long currentSlice = getTimeSlice(timeSliceDurationMs);

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < maxSlices; i++) {
            keys.add(HISTOGRAM_KEY_PREFIX + (currentSlice - (long) i * timeSliceDurationMs));
        }

        return Flux.fromIterable(keys)
                .flatMap(reactiveRedisTemplate::delete)
                .then();
    }
}