package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "queue.weight")
@Data
public class QueueWeightProperties {
    
    private int order = 6;   // ORDER 가중치 (기본값 6)
    private int other = 4;   // OTHER 가중치 (기본값 4)
    
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
