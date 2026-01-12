package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class FeedbackLoopScheduler {

    private final PrometheusMetricsCollector metricsCollector;
    private final GlobalRateLimiterService rateLimiterService;
    private final GlobalQueueService queueService;
    private final FeedbackLoopProperties properties;

    @Value("${prometheus.scrape-interval:30}")
    private int scrapeInterval;

    private final AtomicInteger consecutiveOverloadCount = new AtomicInteger(0);
    private final AtomicInteger consecutiveHealthyCount = new AtomicInteger(0);
    private LocalDateTime lastAdjustmentTime = LocalDateTime.now();

    @Scheduled(fixedDelayString = "${prometheus.scrape-interval:30}000")
    public void feedbackLoop() {
        metricsCollector.collectOrderServiceMetrics()
                .zipWith(queueService.getTotalQueueSize())
                .subscribe(tuple -> {
                    var metrics = tuple.getT1();
                    var queueSize = tuple.getT2();

                    log.info(" Metrics - P95: {}ms, P99: {}ms, Pool: {}%, Queue: {}, Limit: {}",
                            metrics.getP95Latency() * 1000,
                            metrics.getP99Latency() * 1000,
                            metrics.getConnectionPoolUsage(),
                            queueSize,
                            rateLimiterService.getCurrentLimit());

                    evaluateAndAdjust(metrics, queueSize);
                }, error -> {
                    log.error("Failed to collect metrics", error);
                });
    }

    private void evaluateAndAdjust(
            PrometheusMetricsCollector.MetricsSnapshot metrics,
            Long queueSize) {

        double p95Ms = metrics.getP95Latency() * 1000;
        double p99Ms = metrics.getP99Latency() * 1000;
        double connPool = metrics.getConnectionPoolUsage();

        SystemHealthScore healthScore = calculateHealthScore(p95Ms, p99Ms, connPool);

        log.info(" Health Score: {}/100 ({})",
                String.format("%.1f", healthScore.getScore()),
                healthScore.getLevel());

        // UNKNOWN 연결 오류
        if(healthScore.getScore() == -1 ){
            handleUnknown();
        }
        //  심각한 과부하 (Score < 30)
        else if (healthScore.getScore() < 30) {
            handleCriticalOverload(healthScore);
        }
        //  경미한 과부하 (Score < 60)
        else if (healthScore.getScore() < 60) {
            handleModerateOverload(healthScore);
        }
        //  건강 상태 (Score >= 80) + 수요 있음
        else if (healthScore.getScore() >= 80 && queueSize > 0) {
            handleHealthyWithDemand(queueSize);
        }
        //   건강 상태 + 수요 없음
        else {
            handleHealthyNoDemand();
        }
    }

    /**
     * 심각한 과부하 처리 (Score < 30)
     */
    private void handleCriticalOverload(SystemHealthScore score) {
        log.error(" CRITICAL OVERLOAD - Score: {}/100",
                String.format("%.1f", score.getScore()));

        int overloadCount = consecutiveOverloadCount.incrementAndGet();
        consecutiveHealthyCount.set(0);

        if (overloadCount >= 2) {
            int currentLimit = rateLimiterService.getCurrentLimit();

            //  감소량 계산
            int decrease = (int) (currentLimit * properties.getAdjustment().getDecreaseFactor());
            decrease = Math.min(decrease, properties.getAdjustment().getMaxDecrease());  // 최대 30
            decrease = Math.max(decrease, 5);  // 최소 5

            // 최소 제한 확인
            int newLimit = Math.max(
                    currentLimit - decrease,
                    properties.getAdjustment().getMinLimit()
            );
            int actualDecrease = currentLimit - newLimit;

            rateLimiterService.decreaseLimit(actualDecrease);
            lastAdjustmentTime = LocalDateTime.now();
            consecutiveOverloadCount.set(0);

            log.warn("⬇ CRITICAL: {} → {} (-{})",
                    currentLimit, newLimit, actualDecrease);
        }
    }

    /**
     * 경미한 과부하 처리 (Score < 60)
     */
    private void handleModerateOverload(SystemHealthScore score) {
        log.warn(" MODERATE OVERLOAD - Score: {}/100",
                String.format("%.1f", score.getScore()));

        int overloadCount = consecutiveOverloadCount.incrementAndGet();
        consecutiveHealthyCount.set(0);

        if (overloadCount >= 3 &&
                LocalDateTime.now().isAfter(lastAdjustmentTime.plusSeconds(scrapeInterval))) {

            int currentLimit = rateLimiterService.getCurrentLimit();

            //  감소량 계산 (10% 또는 최대 30)
            int decrease = (int) (currentLimit * 0.10);
            decrease = Math.min(decrease, properties.getAdjustment().getMaxDecrease());  // 최대 30
            decrease = Math.max(decrease, 3);  // 최소 3

            int newLimit = Math.max(
                    currentLimit - decrease,
                    properties.getAdjustment().getMinLimit()
            );
            int actualDecrease = currentLimit - newLimit;

            rateLimiterService.decreaseLimit(actualDecrease);
            lastAdjustmentTime = LocalDateTime.now();
            consecutiveOverloadCount.set(0);

            log.warn("⬇ MODERATE: {} → {} (-{})",
                    currentLimit, newLimit, actualDecrease);
        }
    }

    /**
     * 건강 + 수요 있음 → 증가
     */
    private void handleHealthyWithDemand(Long queueSize) {
        log.info(" HEALTHY with demand - Queue: {}", queueSize);

        int healthyCount = consecutiveHealthyCount.incrementAndGet();
        consecutiveOverloadCount.set(0);

        rateLimiterService.isTokenSaturated()
                .subscribe(saturated -> {
                    boolean hasRealDemand = saturated || queueSize > 5;

                    if (hasRealDemand &&
                            healthyCount >= 2 &&
                            LocalDateTime.now().isAfter(
                                    lastAdjustmentTime.plusSeconds(scrapeInterval * 2L))) {

                        int currentLimit = rateLimiterService.getCurrentLimit();

                        //  증가량 계산 (5% 이상)
                        int increase = (int) (currentLimit * properties.getAdjustment().getIncreaseFactor());
                        increase = Math.max(increase, properties.getAdjustment().getMinIncrease());  // 최소 1

                        // 최대 제한 확인
                        int newLimit = Math.min(
                                currentLimit + increase,
                                properties.getAdjustment().getMaxLimit()
                        );
                        int actualIncrease = newLimit - currentLimit;

                        if (actualIncrease > 0) {
                            rateLimiterService.increaseLimit(actualIncrease);
                            lastAdjustmentTime = LocalDateTime.now();
                            consecutiveHealthyCount.set(0);

                            log.info("️ HEALTHY: {} → {} (+{})",
                                    currentLimit, newLimit, actualIncrease);
                        }
                    }
                }, error -> {
                    log.error("Failed to check token saturation", error);
                });
    }

    /**
     * UNKNOWN → 유지
     */
    private void handleUnknown() {
        log.debug(" UNKNOWN - maintaining limit: {}",
                rateLimiterService.getCurrentLimit());
        consecutiveOverloadCount.set(0);
        consecutiveHealthyCount.set(0);
    }

    /**
     * 건강 + 수요 없음 → 기본값까지 점진 감소
     */
    private void handleHealthyNoDemand() {
        int currentLimit = rateLimiterService.getCurrentLimit();
        int baseLimit = properties.getAdjustment().getMinLimit(); // 또는 minLimit

        consecutiveOverloadCount.set(0);
        consecutiveHealthyCount.set(0);

        // 이미 기본값이면 유지
        if (currentLimit <= baseLimit) {
            log.debug(" HEALTHY - No demand, already at base limit: {}", currentLimit);
            return;
        }

        // 20% 감소
        int decrease = (int) (currentLimit * 0.20);
        decrease = Math.max(decrease, 5); // 최소 5 보장

        int newLimit = Math.max(currentLimit - decrease, baseLimit);
        int actualDecrease = currentLimit - newLimit;

        if (actualDecrease > 0) {
            rateLimiterService.decreaseLimit(actualDecrease);
            lastAdjustmentTime = LocalDateTime.now();

            log.info("⬇ IDLE SCALE-IN: {} → {} (-{})",
                    currentLimit, newLimit, actualDecrease);
        }
    }

    /**
     * 시스템 건강 점수 계산 (0~100)
     */
    private SystemHealthScore calculateHealthScore(
            double p95Ms, double p99Ms, double connPool) {

        var latency = properties.getLatency();
        var pool = properties.getPool();

        double p95Score = calculateLatencyScore(
                p95Ms, latency.getP95Good(), latency.getP95Bad()
        );
        double p99Score = calculateLatencyScore(
                p99Ms, latency.getP99Good(), latency.getP99Bad()
        );
        double poolScore = calculatePoolScore(
                connPool, pool.getGood(), pool.getBad()
        );

        // 가중 평균
        double totalScore = (p95Score * 0.3) + (p99Score * 0.4) + (poolScore * 0.3);

        if(p95Ms == 0.0 && p99Ms == 0.0 && connPool == 0.0){
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
}