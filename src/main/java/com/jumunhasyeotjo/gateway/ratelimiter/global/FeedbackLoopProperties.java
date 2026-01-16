package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "feedback-loop")
@Data
public class FeedbackLoopProperties {

    private int intervalMs = 2000;
    private LatencyThresholds latency = new LatencyThresholds();
    private PoolThresholds pool = new PoolThresholds();
    private AdjustmentParams adjustment = new AdjustmentParams();
    private ScaleOutParams scaleOut = new ScaleOutParams();
    private HistogramParams histogram = new HistogramParams();

    @Data
    public static class LatencyThresholds {
        private int p95Good = 500;
        private int p95Bad = 1000;
        private int p99Good = 1000;
        private int p99Bad = 2000;
    }

    @Data
    public static class PoolThresholds {
        private int good = 80;
        private int bad = 95;
    }

    @Data
    public static class AdjustmentParams {
        private double increaseFactor = 0.05;    // 5%
        private double decreaseFactor = 0.25;    // 25%
        private int minLimit = 10;               // 최소 제한
        private int maxLimit = 1000;             // 최대 제한
        private int maxDecrease = 15;            // 감소 최대값
        private int minIncrease = 2;             // 최소 증가량
    }

    @Data
    public static class ScaleOutParams {
        private int targetDelta = 15;                    // target = current + delta
        private int increaseStepSize = 2;                // 느린 증가 단위
        private double decreaseRatio = 0.5;              // 빠른 감소 비율
        private int consecutiveHealthyRequired = 3;      // 증가 전 연속 건강 횟수
        private int consecutiveUnhealthyRequired = 2;    // 감소 전 연속 위험 횟수
    }

    @Data
    public static class HistogramParams {
        private int timeSliceDurationMs = 10000;         // 시간 슬라이스 단위 (10초)
        private int maxSlices = 6;                        // 최대 슬라이스 수 (60초 데이터)
        private int[] bucketBoundaries = {5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};
    }
}
