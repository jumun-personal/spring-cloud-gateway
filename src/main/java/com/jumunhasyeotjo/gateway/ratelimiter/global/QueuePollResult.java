package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

/**
 * DTO for Lua script queue polling result.
 * Contains polled items, statistics, and bucket state.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueuePollResult {

    private List<QueuePollItem> items;
    private QueueStats stats;
    private BucketState bucket;

    public static QueuePollResult empty() {
        return new QueuePollResult(
                Collections.emptyList(),
                new QueueStats(0, 0, 0, 0, 0, 0),
                new BucketState(0.0, 0)
        );
    }

    /**
     * Single item polled from queue.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QueuePollItem {
        private String queue;  // "order_retry", "order_normal", "other_retry", "other_normal"
        private String data;   // JSON serialized QueueItem
        private long score;    // Original timestamp (sorted set score)
    }

    /**
     * Statistics about polled items per queue type.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class QueueStats {
        @JsonProperty("order_retry")
        private int orderRetry;
        @JsonProperty("order_normal")
        private int orderNormal;
        @JsonProperty("other_retry")
        private int otherRetry;
        @JsonProperty("other_normal")
        private int otherNormal;
        @JsonProperty("total_polled")
        private int totalPolled;
        @JsonProperty("remaining_slots")
        private int remainingSlots;
    }

    /**
     * Leaky bucket state after token consumption.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BucketState {
        private double waterLevel;
        private int tokensConsumed;
    }
}
