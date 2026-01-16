package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scale-Out 피드백 루프 스케줄러
 *
 * Scale-Out 이벤트 발생 후 시스템 건강 상태에 따라 limit을 동적으로 조정:
 * - 건강 상태: 느린 증가 (현재 → 목표 limit, 목표 = 현재 + targetDelta)
 * - 위험 상태: 빠른 감소 (현재 → 이전 limit/floor)
 *
 * Redis 히스토그램 기반 P95/P99 레이턴시를 사용하여 건강 상태 판단
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FeedbackLoopScheduler {

    private final PrometheusMetricsCollector prometheusCollector;
    private final RedisLatencyHistogramService histogramService;
    private final GlobalRateLimiterService rateLimiterService;
    private final GlobalQueueService queueService;
    private final FeedbackLoopProperties properties;
    private final FeedbackLoopStateManager stateManager;

    private final AtomicInteger consecutiveHealthyCount = new AtomicInteger(0);
    private final AtomicInteger consecutiveUnhealthyCount = new AtomicInteger(0);
    private LocalDateTime lastAdjustmentTime = LocalDateTime.now();

    /**
     * 피드백 루프 메인 스케줄러
     * Scale-Out 이벤트로 활성화된 경우에만 실행
     */
    @Scheduled(fixedDelayString = "${feedback-loop.interval-ms:2000}")
    public void feedbackLoop() {
        // Scale-Out 이벤트로 활성화된 경우에만 실행
        if (!stateManager.isActive()) {
            return;
        }

        collectMetrics()
                .zipWith(queueService.getTotalQueueSize())
                .subscribe(tuple -> {
                    var metrics = tuple.getT1();
                    var queueSize = tuple.getT2();

                    log.debug("Feedback Loop - P95: {}ms, P99: {}ms, Queue: {}, " +
                                    "Current: {}, Previous(floor): {}, Target: {}",
                            String.format("%.1f", metrics.getP95Latency()),
                            String.format("%.1f", metrics.getP99Latency()),
                            queueSize,
                            rateLimiterService.getCurrentLimit(),
                            stateManager.getPreviousLimit(),
                            stateManager.getTargetLimit());

                    evaluateAndAdjustScaleOut(metrics, queueSize);
                }, error -> {
                    log.error("Failed to collect metrics for feedback loop", error);
                });
    }

    /**
     * Redis 히스토그램에서 메트릭 수집
     * Prometheus 연결 풀 사용량도 함께 수집
     */
    private Mono<MetricsSnapshot> collectMetrics() {
        return Mono.zip(
                histogramService.getP95Latency(),
                histogramService.getP99Latency(),
                prometheusCollector.collectOrderServiceMetrics()
                        .map(PrometheusMetricsCollector.MetricsSnapshot::getConnectionPoolUsage)
                        .onErrorReturn(0.0)
        ).map(tuple -> {
            MetricsSnapshot snapshot = new MetricsSnapshot();
            snapshot.setP95Latency(tuple.getT1());  // 이미 밀리초
            snapshot.setP99Latency(tuple.getT2());
            snapshot.setConnectionPoolUsage(tuple.getT3());
            return snapshot;
        });
    }

    /**
     * Scale-Out 피드백 루프 평가 및 조정
     */
    private void evaluateAndAdjustScaleOut(MetricsSnapshot metrics, Long queueSize) {
        double p95Ms = metrics.getP95Latency();
        double p99Ms = metrics.getP99Latency();
        double connPool = metrics.getConnectionPoolUsage();

        SystemHealthScore healthScore = calculateHealthScore(p95Ms, p99Ms, connPool);

        log.debug("Health Score: {}/100 ({})",
                String.format("%.1f", healthScore.getScore()),
                healthScore.getLevel());

        int currentLimit = rateLimiterService.getCurrentLimit();
        int previousLimit = stateManager.getPreviousLimit();
        var scaleOutParams = properties.getScaleOut();

        // UNKNOWN 상태 (메트릭 없음)
        if (healthScore.getScore() < 0) {
            handleUnknown();
            return;
        }

        // 위험 상태 (score < 50): 빠른 감소
        if (healthScore.getScore() < 50) {
            handleUnhealthyScaleOut(currentLimit, previousLimit, scaleOutParams, healthScore);
            return;
        }

        // 건강 상태 (score >= 80): 느린 증가
        if (healthScore.getScore() >= 80) {
            handleHealthyScaleOut(currentLimit, previousLimit, queueSize, scaleOutParams);
            return;
        }

        // 중간 상태 (50 <= score < 80): 유지
        log.debug("Moderate health ({}) - holding at {}",
                String.format("%.1f", healthScore.getScore()), currentLimit);
        consecutiveHealthyCount.set(0);
        consecutiveUnhealthyCount.set(0);
    }

    /**
     * 위험 상태 처리: 빠른 감소 (floor까지)
     *
     * 감소량 = (현재 - floor) * decreaseRatio
     * 절대 floor 아래로 내려가지 않음
     */
    private void handleUnhealthyScaleOut(int currentLimit, int previousLimit,
                                          FeedbackLoopProperties.ScaleOutParams params,
                                          SystemHealthScore healthScore) {
        int unhealthyCount = consecutiveUnhealthyCount.incrementAndGet();
        consecutiveHealthyCount.set(0);

        log.warn("UNHEALTHY - Score: {}/100, consecutive: {}",
                String.format("%.1f", healthScore.getScore()), unhealthyCount);

        if (unhealthyCount >= params.getConsecutiveUnhealthyRequired()) {
            // 감소량 계산: floor까지 거리의 decreaseRatio만큼 감소
            int distanceToFloor = currentLimit - previousLimit;

            if (distanceToFloor <= 0) {
                log.debug("Already at floor limit: {}", previousLimit);
                consecutiveUnhealthyCount.set(0);
                return;
            }

            int decreaseAmount = (int) Math.ceil(distanceToFloor * params.getDecreaseRatio());
            decreaseAmount = Math.max(decreaseAmount, 1);

            int newLimit = Math.max(currentLimit - decreaseAmount, previousLimit);
            int actualDecrease = currentLimit - newLimit;

            if (actualDecrease > 0) {
                rateLimiterService.decreaseLimit(actualDecrease);
                lastAdjustmentTime = LocalDateTime.now();
                consecutiveUnhealthyCount.set(0);

                log.warn("⬇ SCALE-OUT UNHEALTHY: {} → {} (-{}) [floor={}]",
                        currentLimit, newLimit, actualDecrease, previousLimit);
            }

            // 타겟 리셋 (감소 중이므로)
            stateManager.setTargetLimit(0);
        }
    }

    /**
     * 건강 상태 처리: 느린 증가 (target까지)
     *
     * target = current + targetDelta
     * 증가량 = increaseStepSize
     */
    private void handleHealthyScaleOut(int currentLimit, int previousLimit,
                                        Long queueSize,
                                        FeedbackLoopProperties.ScaleOutParams params) {
        int healthyCount = consecutiveHealthyCount.incrementAndGet();
        consecutiveUnhealthyCount.set(0);

        // 수요 확인 (큐에 대기 중인 요청이 있거나 토큰 포화 상태)
        rateLimiterService.isTokenSaturated()
                .subscribe(saturated -> {
                    boolean hasDemand = queueSize > 0 || saturated;

                    if (!hasDemand) {
                        log.debug("Healthy but no demand - maintaining at {}", currentLimit);
                        return;
                    }

                    if (healthyCount >= params.getConsecutiveHealthyRequired()) {
                        // 타겟 설정 또는 확인
                        int targetLimit = stateManager.getTargetLimit();
                        int maxLimit = properties.getAdjustment().getMaxLimit();

                        if (targetLimit == 0 || currentLimit >= targetLimit) {
                            // 새 타겟 설정 = 현재 + delta
                            targetLimit = Math.min(currentLimit + params.getTargetDelta(), maxLimit);
                            stateManager.setTargetLimit(targetLimit);
                            log.info("New target limit set: {} (current: {}, delta: {})",
                                    targetLimit, currentLimit, params.getTargetDelta());
                        }

                        // 느린 증가
                        if (currentLimit < targetLimit) {
                            int increase = Math.min(
                                    params.getIncreaseStepSize(),
                                    targetLimit - currentLimit
                            );

                            rateLimiterService.increaseLimit(increase);
                            lastAdjustmentTime = LocalDateTime.now();
                            consecutiveHealthyCount.set(0);

                            log.info("⬆ SCALE-OUT HEALTHY: {} → {} (+{}) [target={}, floor={}]",
                                    currentLimit, currentLimit + increase, increase,
                                    targetLimit, previousLimit);
                        }
                    }
                }, error -> {
                    log.error("Failed to check token saturation", error);
                });
    }

    /**
     * UNKNOWN 상태 처리 (메트릭 없음)
     */
    private void handleUnknown() {
        log.debug("UNKNOWN state - maintaining limit: {}",
                rateLimiterService.getCurrentLimit());
        consecutiveHealthyCount.set(0);
        consecutiveUnhealthyCount.set(0);
    }

    /**
     * 시스템 건강 점수 계산 (0~100)
     *
     * 가중 평균:
     * - P95 레이턴시: 30%
     * - P99 레이턴시: 40%
     * - 연결 풀 사용량: 30%
     */
    private SystemHealthScore calculateHealthScore(double p95Ms, double p99Ms, double connPool) {
        var latency = properties.getLatency();
        var pool = properties.getPool();

        double p95Score = calculateLatencyScore(p95Ms, latency.getP95Good(), latency.getP95Bad());
        double p99Score = calculateLatencyScore(p99Ms, latency.getP99Good(), latency.getP99Bad());
        double poolScore = calculatePoolScore(connPool, pool.getGood(), pool.getBad());

        // 가중 평균
        double totalScore = (p95Score * 0.3) + (p99Score * 0.4) + (poolScore * 0.3);

        // 메트릭이 모두 0이면 UNKNOWN
        if (p95Ms == 0.0 && p99Ms == 0.0 && connPool == 0.0) {
            return new SystemHealthScore(-1, p95Score, p99Score, poolScore);
        }

        return new SystemHealthScore(totalScore, p95Score, p99Score, poolScore);
    }

    private double calculateLatencyScore(double actual, double good, double bad) {
        if (actual <= good) {
            return 100.0;
        } else if (actual >= bad) {
            return 0.0;
        } else {
            return 100.0 * (1 - (actual - good) / (bad - good));
        }
    }

    private double calculatePoolScore(double usage, double good, double bad) {
        if (usage <= good) {
            return 100.0;
        } else if (usage >= bad) {
            return 0.0;
        } else {
            return 100.0 * (1 - (usage - good) / (bad - good));
        }
    }

    @Data
    @AllArgsConstructor
    private static class SystemHealthScore {
        private double score;
        private double p95Score;
        private double p99Score;
        private double poolScore;

        public String getLevel() {
            if (score >= 80) return "EXCELLENT";
            if (score >= 60) return "GOOD";
            if (score >= 30) return "DEGRADED";
            if (score == -1) return "UNKNOWN";
            return "CRITICAL";
        }
    }

    @Data
    private static class MetricsSnapshot {
        private double p95Latency;
        private double p99Latency;
        private double connectionPoolUsage;
    }
}