package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class GlobalQueueService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final ObjectMapper objectMapper;
    private final GlobalRateLimiterService rateLimiterService;

    @Value("classpath:scripts/queue_weighting.lua")
    private Resource queueWeightingScript;

    private RedisScript<String> weightedPollScript;

    public GlobalQueueService(ReactiveRedisTemplate<String, String> reactiveRedisTemplate,
                              ObjectMapper objectMapper,
                              GlobalRateLimiterService rateLimiterService) {
        this.reactiveRedisTemplate = reactiveRedisTemplate;
        this.objectMapper = objectMapper;
        this.rateLimiterService = rateLimiterService;
    }

    @PostConstruct
    public void init() throws IOException {
        if (queueWeightingScript.exists()) {
            String scriptContent = queueWeightingScript.getContentAsString(StandardCharsets.UTF_8);
            weightedPollScript = RedisScript.of(scriptContent, String.class);
            log.info("Loaded queue_weighting.lua script");
        } else {
            log.warn("queue_weighting.lua script not found in classpath");
        }
    }

    private static final String ORDER_QUEUE_KEY = "queue:global:order";
    private static final String OTHER_QUEUE_KEY = "queue:global:other";
    private static final String ORDER_RETRY_QUEUE_KEY = "queue:global:order:retry";
    private static final String OTHER_RETRY_QUEUE_KEY = "queue:global:other:retry";

    @Getter
    public enum QueueType {
        ORDER(ORDER_QUEUE_KEY, ORDER_RETRY_QUEUE_KEY),
        OTHER(OTHER_QUEUE_KEY, OTHER_RETRY_QUEUE_KEY);

        private final String key;
        private final String retryKey;

        QueueType(String key, String retryKey) {
            this.key = key;
            this.retryKey = retryKey;
        }
    }

    public QueueType resolveQueueType(HttpMethod method, String uri) {
        if (uri != null && uri.contains("/orders") && method.equals(HttpMethod.POST)) {
            return QueueType.ORDER;
        }
        return QueueType.OTHER;
    }

    public Mono<Boolean> offer(QueueItem item, QueueType queueType) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(item))
                .flatMap(value -> {
                    double score = item.getOriginalTimestamp();
                    return reactiveRedisTemplate.opsForZSet().add(queueType.getKey(), value, score);
                })
                .doOnNext(added -> log.debug("Queue offer [{}]: {}", queueType, added));
    }

    public Mono<Boolean> offerToRetry(QueueItem item, QueueType queueType) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(item))
                .flatMap(value -> {
                    double score = item.getOriginalTimestamp();
                    return reactiveRedisTemplate.opsForZSet().add(queueType.getRetryKey(), value, score);
                })
                .doOnNext(added -> log.debug("Retry queue offer [{}]: {}", queueType, added));
    }

    public Mono<List<QueueItem>> poll(QueueType queueType, int size) {
        return pollFromKey(queueType.getKey(), size);
    }

    public Mono<List<QueueItem>> pollFromRetry(QueueType queueType, int size) {
        return pollFromKey(queueType.getRetryKey(), size);
    }

    /**
     * 재시도 큐에서 4초 이상 경과한 아이템만 조회
     */
    public Mono<List<QueueItem>> pollRetryEligible(QueueType queueType, int size) {
        long threshold = System.currentTimeMillis() - 4000; // 4초 전
        
        return reactiveRedisTemplate.opsForZSet()
                .rangeByScore(queueType.getRetryKey(), Range.closed(0.0, (double) threshold))
                .take(size)
                .collectList()
                .flatMap(items -> {
                    if (items.isEmpty()) {
                        return Mono.just(Collections.<QueueItem>emptyList());
                    }

                    List<QueueItem> result = items.stream()
                            .map(this::deserialize)
                            .collect(Collectors.toList());

                    return reactiveRedisTemplate.opsForZSet()
                            .remove(queueType.getRetryKey(), items.toArray())
                            .thenReturn(result);
                });
    }

    private Mono<List<QueueItem>> pollFromKey(String key, int size) {
        if (size <= 0) {
            return Mono.just(Collections.emptyList());
        }

        return reactiveRedisTemplate.opsForZSet()
                .range(key, Range.closed(0L, (long) size - 1))
                .collectList()
                .flatMap(items -> {
                    if (items.isEmpty()) {
                        return Mono.just(Collections.<QueueItem>emptyList());
                    }

                    List<QueueItem> result = items.stream()
                            .map(this::deserialize)
                            .collect(Collectors.toList());

                    return reactiveRedisTemplate.opsForZSet()
                            .remove(key, items.toArray())
                            .thenReturn(result);
                });
    }

    public Mono<Long> findSequence(Long userId, QueueType queueType) {
        return reactiveRedisTemplate.opsForZSet()
                .range(queueType.getKey(), Range.closed(0L, Long.MAX_VALUE))
                .collectList()
                .map(allItems -> {
                    long position = 0;
                    for (String item : allItems) {
                        QueueItem queueItem = deserialize(item);
                        if (queueItem.getUserId().equals(userId)) {
                            return position;
                        }
                        position++;
                    }
                    return -1L;
                });
    }

    public Mono<Long> getQueueSize(QueueType queueType) {
        return reactiveRedisTemplate.opsForZSet()
                .size(queueType.getKey())
                .defaultIfEmpty(0L);
    }

    public Mono<Long> getRetryQueueSize(QueueType queueType) {
        return reactiveRedisTemplate.opsForZSet()
                .size(queueType.getRetryKey())
                .defaultIfEmpty(0L);
    }

    /**
     * 4초 이상 경과한 재시도 대기 아이템 수
     */
    public Mono<Long> getRetryEligibleCount(QueueType queueType) {
        long threshold = System.currentTimeMillis() - 4000;
        return reactiveRedisTemplate.opsForZSet()
                .count(queueType.getRetryKey(), Range.closed(0.0, (double) threshold))
                .defaultIfEmpty(0L);
    }

    public Mono<Long> getTotalQueueSize() {
        return getQueueSize(QueueType.ORDER)
                .zipWith(getQueueSize(QueueType.OTHER))
                .map(tuple -> tuple.getT1() + tuple.getT2());
    }

    public Mono<Long> getTotalRetryQueueSize() {
        return getRetryQueueSize(QueueType.ORDER)
                .zipWith(getRetryQueueSize(QueueType.OTHER))
                .map(tuple -> tuple.getT1() + tuple.getT2());
    }

    private QueueItem deserialize(String json) {
        try {
            return objectMapper.readValue(json, QueueItem.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize QueueItem: {}", json, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Atomically poll items from all queues using weighted distribution.
     * Reduces 12-16 Redis calls to 1 single Lua script execution.
     *
     * Priority order:
     * 1. ORDER retry queue (eligible items only - aged >= 4 seconds)
     * 2. ORDER normal queue
     * 3. OTHER retry queue (eligible items only)
     * 4. OTHER normal queue
     *
     * @param totalSlots Maximum number of items to fetch
     * @param config Weight configuration (order, other, retryRatio)
     * @return QueuePollResult containing polled items and statistics
     */
    public Mono<QueuePollResult> pollWeighted(int totalSlots, QueueWeightProperties config) {
        if (weightedPollScript == null) {
            log.warn("Weighted poll script not loaded, returning empty result");
            return Mono.just(QueuePollResult.empty());
        }

        if (totalSlots <= 0) {
            return Mono.just(QueuePollResult.empty());
        }

        long now = System.currentTimeMillis();
        long retryThreshold = now - 4000; // 4 seconds eligibility

        List<String> keys = Arrays.asList(
                ORDER_QUEUE_KEY,
                ORDER_RETRY_QUEUE_KEY,
                OTHER_QUEUE_KEY,
                OTHER_RETRY_QUEUE_KEY,
                "leaky:global:bucket"
        );

        List<String> args = Arrays.asList(
                String.valueOf(now),
                String.valueOf(totalSlots),
                String.valueOf(config.getOrder()),
                String.valueOf(config.getOther()),
                String.valueOf(config.getRetryRatio()),
                String.valueOf(retryThreshold),
                String.valueOf(rateLimiterService.getCurrentLimit()),  // leak rate
                String.valueOf(rateLimiterService.getCurrentLimit())   // capacity
        );

        return reactiveRedisTemplate.execute(
                        weightedPollScript,
                        keys,
                        args.toArray(new String[0])
                )
                .next()
                .map(this::parseQueuePollResult)
                .doOnNext(result -> log.debug("Weighted poll: {} items from Lua script",
                        result.getStats().getTotalPolled()))
                .onErrorResume(e -> {
                    log.error("Weighted poll failed, returning empty result", e);
                    return Mono.just(QueuePollResult.empty());
                });
    }

    /**
     * Parse JSON result from Lua script into QueuePollResult.
     */
    private QueuePollResult parseQueuePollResult(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);

            // Parse items
            List<QueuePollResult.QueuePollItem> items = new ArrayList<>();
            JsonNode itemsNode = root.get("items");
            if (itemsNode != null && itemsNode.isArray()) {
                for (JsonNode itemNode : itemsNode) {
                    QueuePollResult.QueuePollItem item = new QueuePollResult.QueuePollItem(
                            itemNode.get("queue").asText(),
                            itemNode.get("data").asText(),
                            itemNode.get("score").asLong()
                    );
                    items.add(item);
                }
            }

            // Parse stats
            JsonNode statsNode = root.get("stats");
            QueuePollResult.QueueStats stats = new QueuePollResult.QueueStats(
                    statsNode.get("order_retry").asInt(),
                    statsNode.get("order_normal").asInt(),
                    statsNode.get("other_retry").asInt(),
                    statsNode.get("other_normal").asInt(),
                    statsNode.get("total_polled").asInt(),
                    statsNode.get("remaining_slots").asInt()
            );

            // Parse bucket state
            JsonNode bucketNode = root.get("bucket");
            QueuePollResult.BucketState bucket = new QueuePollResult.BucketState(
                    bucketNode.get("water_level").asDouble(),
                    bucketNode.get("tokens_consumed").asInt()
            );

            return new QueuePollResult(items, stats, bucket);
        } catch (Exception e) {
            log.error("Failed to parse Lua script result: {}", json, e);
            return QueuePollResult.empty();
        }
    }

    /**
     * Check if weighted polling is available (Lua script loaded).
     */
    public boolean isWeightedPollAvailable() {
        return weightedPollScript != null;
    }
}
