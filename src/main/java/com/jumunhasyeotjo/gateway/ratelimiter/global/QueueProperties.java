package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "queue")
@Data
public class QueueProperties {

    private Weight weight = new Weight();
    private Retry retry = new Retry();
    private int maxSize = 10_000_000;
    private int processorIntervalMs = 100;

    @Data
    public static class Weight {
        private int order = 7;
        private int other = 3;
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
    }

    @Data
    public static class Retry {
        private int eligibleAfterSeconds = 4;
        private double maxSlotRatio = 0.2;
        private int maxRetryCount = 1;
    }
}
