package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ratelimit")
@Data
public class LeakyBucketProperties {

    private Global global = new Global();
    private Pg pg = new Pg();

    @Data
    public static class Global {
        private volatile int rate = 15;        // 초당 처리량
        private volatile int capacity = 15;    // = rate (버스트 불허)
        
        // Feedback Loop에서 동적 변경 가능
        public synchronized void setRate(int rate) {
            this.rate = rate;
        }
        
        public synchronized void setCapacity(int capacity) {
            this.capacity = capacity;
        }
    }

    @Data
    public static class Pg {
        private Toss toss = new Toss();

        @Data
        public static class Toss {
            private int rate = 10;
            private int capacity = 10;
        }
    }
}
