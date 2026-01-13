package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "feedback-loop")
@Data
public class FeedbackLoopProperties {

    private int intervalMs = 2000;
    private Latency latency = new Latency();
    private Pool pool = new Pool();
    private Adjustment adjustment = new Adjustment();

    @Data
    public static class Latency {
        private double p95Good = 200;   // ms
        private double p95Bad = 1000;   // ms
        private double p99Good = 500;   // ms
        private double p99Bad = 2000;   // ms
    }

    @Data
    public static class Pool {
        private double good = 50;   // %
        private double bad = 90;    // %
    }

    @Data
    public static class Adjustment {
        private int minLimit = 10;
        private int maxLimit = 100;
        private double increaseFactor = 0.05;
        private double decreaseFactor = 0.15;
        private int minIncrease = 1;
        private int maxDecrease = 20;
    }
}
