package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "queue.weight")
@Data
public class QueueWeightProperties {

    private int order = 7;   // ORDER 가중치 (기본값 7)
    private int other = 3;   // OTHER 가중치 (기본값 3)
    private double retryRatio = 0.7;  // retry : normal = 7 : 3

    public int getTotal() {
        return order + other;
    }

    public double getOrderRatio() {
        return (double) order / getTotal();
    }

    public double getOtherRatio() {
        return (double) other / getTotal();
    }

    /**
     * Get retry ratio within each queue group (ORDER or OTHER).
     * Value is clamped between 0.0 and 1.0.
     */
    public double getRetryRatio() {
        return Math.max(0.0, Math.min(1.0, retryRatio));
    }
}
