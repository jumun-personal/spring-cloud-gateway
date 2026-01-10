package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "feedback-loop")
@Data
public class FeedbackLoopProperties {

    private LatencyThresholds latency = new LatencyThresholds();
    private PoolThresholds pool = new PoolThresholds();
    private AdjustmentParams adjustment = new AdjustmentParams();

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
        private int maxDecrease = 30;            // 감소 최대값 (새로 추가!)
        private int minIncrease = 1;             // 최소 증가량
    }
}
