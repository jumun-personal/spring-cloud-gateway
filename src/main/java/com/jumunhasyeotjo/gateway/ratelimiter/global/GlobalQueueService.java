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
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class GlobalQueueService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final ObjectMapper objectMapper;
    private final GlobalRateLimiterService rateLimiterService;
    // ===== Global-only queue keys (PG 불필요) =====
    private static final String G_ORDER_KEY       = "queue:global:order";
    private static final String G_ORDER_RETRY_KEY = "queue:global:order:retry";
    private static final String G_OTHER_KEY       = "queue:global:other";
    private static final String G_OTHER_RETRY_KEY = "queue:global:other:retry";

    // ===== PG-required queue keys (PG 필요) =====
    private static final String PG_ORDER_KEY       = "queue:pg:order";
    private static final String PG_ORDER_RETRY_KEY = "queue:pg:order:retry";
    private static final String PG_OTHER_KEY       = "queue:pg:other";
    private static final String PG_OTHER_RETRY_KEY = "queue:pg:other:retry";

    @Value("classpath:scripts/queue_weighting.lua")
    private Resource queueWeightingScript;
    private final String WEIGHTED_POLL_ONLY_LUA = """
            --[[
              Weighted Queue Polling (POLL-ONLY)
              - NO token bucket update here (reserved in Java)
              - Redis round trips: 1 (EVAL)
              - Complexity: O(M log N)
            ]]
            
            local orderNormalKey = KEYS[1]
            local orderRetryKey  = KEYS[2]
            local otherNormalKey = KEYS[3]
            local otherRetryKey  = KEYS[4]
            
            local now            = tonumber(ARGV[1])
            local totalSlots     = tonumber(ARGV[2])
            local orderWeight    = tonumber(ARGV[3])
            local otherWeight    = tonumber(ARGV[4])
            local retryRatio     = tonumber(ARGV[5])
            local retryThreshold = tonumber(ARGV[6])
            
            if totalSlots <= 0 then
              return cjson.encode({
                items = {},
                stats = { order_retry=0, order_normal=0, other_retry=0, other_normal=0, total_polled=0, remaining_slots=0 }
              })
            end
            
            local result = {
              items = {},
              stats = { order_retry=0, order_normal=0, other_retry=0, other_normal=0, total_polled=0, remaining_slots=totalSlots }
            }
            
            local function pollFromQueue(key, count, isRetry, threshold)
              if count <= 0 then return {} end
            
              local items
              if isRetry then
                items = redis.call('ZRANGEBYSCORE', key, '-inf', threshold, 'WITHSCORES', 'LIMIT', 0, count)
              else
                items = redis.call('ZRANGE', key, 0, count - 1, 'WITHSCORES')
              end
            
              local polled = {}
              local toRemove = {}
            
              for i = 1, #items, 2 do
                local member = items[i]
                local score = items[i + 1]
                table.insert(polled, { data = member, score = tonumber(score) })
                table.insert(toRemove, member)
              end
            
              if #toRemove > 0 then
                redis.call('ZREM', key, unpack(toRemove))
              end
            
              return polled
            end
            
            local function calculateSlots(total, orderW, otherW, retryR)
              local totalWeight = orderW + otherW
              local orderTotal = math.floor(total * orderW / totalWeight + 0.5)
              local otherTotal = total - orderTotal
            
              local orderRetrySlots  = math.floor(orderTotal * retryR + 0.5)
              local orderNormalSlots = orderTotal - orderRetrySlots
            
              local otherRetrySlots  = math.floor(otherTotal * retryR + 0.5)
              local otherNormalSlots = otherTotal - otherRetrySlots
            
              return {
                order_retry  = orderRetrySlots,
                order_normal = orderNormalSlots,
                other_retry  = otherRetrySlots,
                other_normal = otherNormalSlots
              }
            end
            
            local slots = calculateSlots(totalSlots, orderWeight, otherWeight, retryRatio)
            local remainingSlots = totalSlots
            local polledItems = {}
            
            -- 1) ORDER retry
            local orderRetryItems = pollFromQueue(orderRetryKey, slots.order_retry, true, retryThreshold)
            for _, it in ipairs(orderRetryItems) do it.queue='order_retry'; table.insert(polledItems, it) end
            result.stats.order_retry = #orderRetryItems
            remainingSlots = remainingSlots - #orderRetryItems
            
            -- ORDER retry unused -> ORDER normal
            local orderNormalSlots = slots.order_normal + (slots.order_retry - #orderRetryItems)
            
            -- 2) ORDER normal
            local orderNormalItems = pollFromQueue(orderNormalKey, orderNormalSlots, false, 0)
            for _, it in ipairs(orderNormalItems) do it.queue='order_normal'; table.insert(polledItems, it) end
            result.stats.order_normal = #orderNormalItems
            remainingSlots = remainingSlots - #orderNormalItems
            
            -- ORDER unused -> OTHER split by retryRatio
            local orderTotalUsed = #orderRetryItems + #orderNormalItems
            local orderTotalAllocated = slots.order_retry + slots.order_normal
            local orderUnused = orderTotalAllocated - orderTotalUsed
            if orderUnused < 0 then orderUnused = 0 end
            
            local extraOtherRetry = math.floor(orderUnused * retryRatio + 0.5)
            local extraOtherNormal = orderUnused - extraOtherRetry
            
            -- 3) OTHER retry
            local otherRetrySlots = slots.other_retry + extraOtherRetry
            local otherRetryItems = pollFromQueue(otherRetryKey, otherRetrySlots, true, retryThreshold)
            for _, it in ipairs(otherRetryItems) do it.queue='other_retry'; table.insert(polledItems, it) end
            result.stats.other_retry = #otherRetryItems
            remainingSlots = remainingSlots - #otherRetryItems
            
            -- OTHER retry unused -> OTHER normal (+ extraOtherNormal)
            local otherRetryUnused = otherRetrySlots - #otherRetryItems
            if otherRetryUnused < 0 then otherRetryUnused = 0 end
            
            local otherNormalSlots = slots.other_normal + otherRetryUnused + extraOtherNormal
            
            -- 4) OTHER normal
            local otherNormalItems = pollFromQueue(otherNormalKey, otherNormalSlots, false, 0)
            for _, it in ipairs(otherNormalItems) do it.queue='other_normal'; table.insert(polledItems, it) end
            result.stats.other_normal = #otherNormalItems
            remainingSlots = remainingSlots - #otherNormalItems
            
            result.stats.total_polled = #polledItems
            result.stats.remaining_slots = remainingSlots
            result.items = polledItems
            
            return cjson.encode(result)
            """;

    private RedisScript<String> weightedPollScript;
    private RedisScript<String> weightedPollOnlyScript;

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
        weightedPollOnlyScript = RedisScript.of(WEIGHTED_POLL_ONLY_LUA, String.class);
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

    public Mono<QueuePollResult> pollWeightedPg(int totalSlots, QueueWeightProperties props) {
        List<String> keys = List.of(
                PG_ORDER_KEY,
                PG_ORDER_RETRY_KEY,
                PG_OTHER_KEY,
                PG_OTHER_RETRY_KEY
        );
        return pollWeightedInternal(keys, totalSlots, props);
    }

    public Mono<QueuePollResult> pollWeightedGlobalOnly(int totalSlots, QueueWeightProperties props) {
        List<String> keys = List.of(
                G_ORDER_KEY,
                G_ORDER_RETRY_KEY,
                G_OTHER_KEY,
                G_OTHER_RETRY_KEY
        );
        return pollWeightedInternal(keys, totalSlots, props);
    }

    private Mono<QueuePollResult> pollWeightedInternal(
            List<String> keys,
            int totalSlots,
            QueueWeightProperties props
    ) {
        if (totalSlots <= 0) return Mono.just(QueuePollResult.empty());

        long now = System.currentTimeMillis();
        long retryThreshold = now - props.getRetryDelayMs(); // 예: 4000ms

        // ARGV 순서 (Lua와 반드시 일치)
        // ARGV[1]=now, [2]=totalSlots, [3]=orderWeight, [4]=otherWeight, [5]=retryRatio, [6]=retryThreshold
        return reactiveRedisTemplate.execute(
                        weightedPollOnlyScript,
                        keys,
                        String.valueOf(now),
                        String.valueOf(totalSlots),
                        String.valueOf(props.getOrder()),
                        String.valueOf(props.getOther()),
                        String.valueOf(props.getRetryRatio()),
                        String.valueOf(retryThreshold)
                )
                .next()
                .switchIfEmpty(Mono.just("{}"))
                .flatMap(this::parsePollResultSafely);
    }

    private Mono<QueuePollResult> parsePollResultSafely(String json) {
        try {
            QueuePollResult result = objectMapper.readValue(json, QueuePollResult.class);
            if (result == null) return Mono.just(QueuePollResult.empty());
            if (result.getItems() == null) result.setItems(Collections.emptyList());
            if (result.getStats() == null) result.setStats(new QueuePollResult.QueueStats(0,0,0,0,0,0));
            if (result.getBucket() == null) result.setBucket(new QueuePollResult.BucketState(0.0, 0));
            return Mono.just(result);
        } catch (Exception e) {
            return Mono.just(QueuePollResult.empty());
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
