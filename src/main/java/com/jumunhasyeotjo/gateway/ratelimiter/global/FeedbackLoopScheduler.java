package com.jumunhasyeotjo.gateway.ratelimiter.global;

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

    @Value("${prometheus.scrape-interval:15}") // Prometheus 스크랩 주기 (초)
    private int scrapeInterval;

    private final AtomicInteger consecutiveOverloadCount = new AtomicInteger(0);
    private LocalDateTime lastScaleUpTime = LocalDateTime.now();

    // Prometheus 갱신 주기마다 실행
    @Scheduled(fixedDelayString = "${prometheus.scrape-interval:15}000")
    public void feedbackLoop() {
        metricsCollector.collectOrderServiceMetrics()
                .subscribe(metrics -> {
                    log.info("Metrics collected - P95: {}ms, P99: {}ms, ConnPool: {}%",
                            metrics.getP95Latency() * 1000,
                            metrics.getP99Latency() * 1000,
                            metrics.getConnectionPoolUsage());

                    evaluateAndAdjust(metrics);
                });
    }

    private void evaluateAndAdjust(PrometheusMetricsCollector.MetricsSnapshot metrics) {
        double p95Ms = metrics.getP95Latency() * 1000;
        double p99Ms = metrics.getP99Latency() * 1000;
        double connPool = metrics.getConnectionPoolUsage();

        // 증가 조건: P99 < 1000ms && P95 < 500ms && ConnPool < 80%
        if (p99Ms < 1000 && p95Ms < 500 && connPool < 80 ) {
            // 지연을 고려하여 Prometheus 갱신 주기 * 2 대기
            if (LocalDateTime.now().isAfter(
                    lastScaleUpTime.plusSeconds(scrapeInterval * 2))) {
                rateLimiterService.increaseLimit();
                lastScaleUpTime = LocalDateTime.now();
                consecutiveOverloadCount.set(0);
            }
        } else {
            // 과부하 감지
            log.warn("System under pressure - P95: {}ms, P99: {}ms, ConnPool: {}%",
                    p95Ms, p99Ms, connPool);

            int overloadCount = consecutiveOverloadCount.incrementAndGet();

            // 연속 과부하 시 Rate Limit 감소
            if (overloadCount >= 3) {
                rateLimiterService.decreaseLimit(5);
                consecutiveOverloadCount.set(0);
            }
        }
    }
}
