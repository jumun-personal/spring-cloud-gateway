package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Feedback Loop - Scale Out 이벤트 기반
 *
 * 트리거: Scale Out API 호출 시 활성화
 * 증가: 현재 → 목표 (현재 + 15) 천천히
 * 감소: 현재 → 이전 limit (최소값) 빠르게
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FeedbackLoopScheduler {

    private final LatencyHistogramService latencyHistogramService;
    private final GlobalRateLimiterService rateLimiterService;
    private final GlobalQueueService queueService;
    private final LeakyBucketProperties leakyBucketProperties;
    private final FeedbackLoopProperties properties;

    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final AtomicInteger previousLimit = new AtomicInteger(30);
    private final AtomicInteger targetLimit = new AtomicInteger(45);
    private final AtomicInteger consecutiveHealthyCount = new AtomicInteger(0);
    private final AtomicInteger consecutiveUnhealthyCount = new AtomicInteger(0);
    private LocalDateTime lastAdjustmentTime = LocalDateTime.now();

    private static final int TARGET_OFFSET = 15;
    private static final double INCREASE_RATE = 0.05;  // 5% 증가
    private static final double DECREASE_RATE = 0.15;  // 15% 감소

    /**
     * Scale Out 이벤트로 Feedback Loop 시작
     */
    public void startFeedbackLoop() {
        int currentLimit = leakyBucketProperties.getGlobal().getCapacity();
        
        previousLimit.set(currentLimit);
        targetLimit.set(currentLimit + TARGET_OFFSET);
        isActive.set(true);
        
        consecutiveHealthyCount.set(0);
        consecutiveUnhealthyCount.set(0);
        lastAdjustmentTime = LocalDateTime.now();
        
        // Histogram 초기화
        latencyHistogramService.reset().subscribe();
        
        log.info("Feedback Loop started - previous: {}, target: {}", 
                previousLimit.get(), targetLimit.get());
    }

    /**
     * Feedback Loop 중지
     */
    public void stopFeedbackLoop() {
        isActive.set(false);
        log.info("Feedback Loop stopped - current limit: {}", 
                leakyBucketProperties.getGlobal().getCapacity());
    }

    /**
     * 주기적 실행 (활성화 상태일 때만)
     */
    //@Scheduled(fixedDelayString = "${feedback-loop.interval-ms:2000}")
    public void feedbackLoop() {
        if (!isActive.get()) {
            return;
        }

        latencyHistogramService.getPercentiles()
                .zipWith(queueService.getTotalQueueSize())
                .subscribe(tuple -> {
                    var percentiles = tuple.getT1();
                    var queueSize = tuple.getT2();

                    log.debug("Feedback Loop - P95: {}ms, P99: {}ms, Queue: {}, Limit: {}",
                            percentiles.getP95(),
                            percentiles.getP99(),
                            queueSize,
                            leakyBucketProperties.getGlobal().getCapacity());

                    evaluateAndAdjust(percentiles.getP95(), percentiles.getP99(), queueSize);
                }, error -> {
                    log.error("Feedback Loop error", error);
                });
    }

    private void evaluateAndAdjust(double p95Ms, double p99Ms, Long queueSize) {
        var latency = properties.getLatency();
        int currentLimit = leakyBucketProperties.getGlobal().getCapacity();
        int prevLimit = previousLimit.get();
        int target = targetLimit.get();

        // 건강 상태 판단
        boolean isHealthy = p95Ms <= latency.getP95Good() && p99Ms <= latency.getP99Good();
        boolean isCritical = p95Ms >= latency.getP95Bad() || p99Ms >= latency.getP99Bad();

        if (isCritical) {
            handleCritical(currentLimit, prevLimit, p95Ms, p99Ms);
        } else if (!isHealthy) {
            handleModerate(currentLimit, prevLimit, p95Ms, p99Ms);
        } else if (queueSize > 0 && currentLimit < target) {
            handleHealthyWithDemand(currentLimit, target);
        } else {
            handleHealthyNoDemand(currentLimit);
        }
    }

    /**
     * Critical 상태 - 빠른 감소
     */
    private void handleCritical(int currentLimit, int minLimit, double p95, double p99) {
        consecutiveHealthyCount.set(0);
        int unhealthyCount = consecutiveUnhealthyCount.incrementAndGet();

        log.warn("CRITICAL - P95: {}ms, P99: {}ms, count: {}", p95, p99, unhealthyCount);

        if (unhealthyCount >= 2) {
            // 15% 감소, 최소값은 이전 limit
            int decrease = (int) Math.max(currentLimit * DECREASE_RATE, 3);
            int newLimit = Math.max(currentLimit - decrease, minLimit);

            if (newLimit < currentLimit) {
                updateLimit(newLimit);
                log.warn("⬇ CRITICAL: {} → {} (min: {})", currentLimit, newLimit, minLimit);
                consecutiveUnhealthyCount.set(0);
            }
        }
    }

    /**
     * Moderate 상태 - 적당한 감소
     */
    private void handleModerate(int currentLimit, int minLimit, double p95, double p99) {
        consecutiveHealthyCount.set(0);
        int unhealthyCount = consecutiveUnhealthyCount.incrementAndGet();

        log.debug("MODERATE - P95: {}ms, P99: {}ms, count: {}", p95, p99, unhealthyCount);

        if (unhealthyCount >= 3) {
            // 10% 감소, 최소값은 이전 limit
            int decrease = (int) Math.max(currentLimit * 0.10, 2);
            int newLimit = Math.max(currentLimit - decrease, minLimit);

            if (newLimit < currentLimit) {
                updateLimit(newLimit);
                log.warn("⬇ MODERATE: {} → {} (min: {})", currentLimit, newLimit, minLimit);
                consecutiveUnhealthyCount.set(0);
            }
        }
    }

    /**
     * Healthy + 대기열 있음 - 느린 증가
     */
    private void handleHealthyWithDemand(int currentLimit, int maxLimit) {
        consecutiveUnhealthyCount.set(0);
        int healthyCount = consecutiveHealthyCount.incrementAndGet();

        if (healthyCount >= 5 && currentLimit < maxLimit) {
            // 5% 증가, 최대값은 target limit
            int increase = (int) Math.max(currentLimit * INCREASE_RATE, 1);
            int newLimit = Math.min(currentLimit + increase, maxLimit);

            if (newLimit > currentLimit) {
                updateLimit(newLimit);
                log.info("⬆ HEALTHY: {} → {} (max: {})", currentLimit, newLimit, maxLimit);
                consecutiveHealthyCount.set(0);
            }
        }
    }

    /**
     * Healthy + 대기열 없음 - 유지
     */
    private void handleHealthyNoDemand(int currentLimit) {
        consecutiveUnhealthyCount.set(0);
        consecutiveHealthyCount.set(0);
        log.debug("HEALTHY - No demand, maintaining limit: {}", currentLimit);
    }

    private void updateLimit(int newLimit) {
        // LeakyBucketProperties의 capacity와 rate 동시 업데이트
        leakyBucketProperties.getGlobal().setCapacity(newLimit);
        leakyBucketProperties.getGlobal().setRate(newLimit);
        lastAdjustmentTime = LocalDateTime.now();
    }

    // Getters for monitoring
    public boolean isActive() {
        return isActive.get();
    }

    public int getPreviousLimit() {
        return previousLimit.get();
    }

    public int getTargetLimit() {
        return targetLimit.get();
    }
}
