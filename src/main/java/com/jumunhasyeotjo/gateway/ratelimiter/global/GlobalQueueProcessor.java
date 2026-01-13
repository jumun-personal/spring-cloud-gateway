package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumunhasyeotjo.gateway.ratelimiter.HttpRequestData;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalQueueService.QueueType;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.ReactiveRateLimiterService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ConnectTimeoutException;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.ConnectException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
@RequiredArgsConstructor
public class GlobalQueueProcessor {

    private final GlobalQueueService globalQueueService;
    private final GlobalRateLimiterService globalRateLimiterService;
    private final ReactiveRateLimiterService pgRateLimiterService;
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final QueueProperties queueProperties;
    private final ObjectMapper objectMapper;
    private final WebClient webClient;
    private final MeterRegistry meterRegistry;

    private static final String DEFAULT_PROVIDER = "TOSS";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);

    private static final String ORDER_QUEUE_KEY = "queue:global:order";
    private static final String OTHER_QUEUE_KEY = "queue:global:other";
    private static final String ORDER_RETRY_KEY = "queue:global:order:retry";
    private static final String OTHER_RETRY_KEY = "queue:global:other:retry";

    // 메트릭
    private Timer queueWaitTimer;
    private Counter retrySuccessCounter;
    private Counter retryFailCounter;
    private Counter firstAttemptSuccessCounter;

    /**
     * 가중치 기반 큐 처리 Lua Script
     */
    private static final String WEIGHTED_POLL_LUA = """
        local orderQueueKey = KEYS[1]
        local otherQueueKey = KEYS[2]
        local orderRetryKey = KEYS[3]
        local otherRetryKey = KEYS[4]
        
        local totalSlots = tonumber(ARGV[1])
        local orderWeight = tonumber(ARGV[2])
        local otherWeight = tonumber(ARGV[3])
        local retryRatio = tonumber(ARGV[4])
        local retryThreshold = tonumber(ARGV[5])
        
        local result = {
            orderRetry  = cjson.empty_array,
            orderNormal = cjson.empty_array,
            otherRetry  = cjson.empty_array,
            otherNormal = cjson.empty_array
        }
        
        -- 가중치 계산
        local totalWeight = orderWeight + otherWeight
        local orderSlots = math.ceil(totalSlots * orderWeight / totalWeight)
        local otherSlots = totalSlots - orderSlots
        
        -- ORDER 슬롯 내 retry/normal 분배 (retry 우선, 7:3)
        local orderRetrySlots = math.ceil(orderSlots * retryRatio)
        local orderNormalSlots = orderSlots - orderRetrySlots
        
        -- OTHER 슬롯 내 retry/normal 분배
        local otherRetrySlots = math.ceil(otherSlots * retryRatio)
        local otherNormalSlots = otherSlots - otherRetrySlots
        
        -- 1. ORDER retry (eligible만)
        if orderRetrySlots > 0 then
            local items = redis.call('ZRANGEBYSCORE', orderRetryKey, 0, retryThreshold, 'LIMIT', 0, orderRetrySlots)
            if #items > 0 then
                for _, item in ipairs(items) do
                    table.insert(result.orderRetry, item)
                end
                redis.call('ZREM', orderRetryKey, unpack(items))
            end
            orderNormalSlots = orderNormalSlots + (orderRetrySlots - #items)
        end
        
        -- 2. ORDER normal
        if orderNormalSlots > 0 then
            local items = redis.call('ZRANGE', orderQueueKey, 0, orderNormalSlots - 1)
            if #items > 0 then
                for _, item in ipairs(items) do
                    table.insert(result.orderNormal, item)
                end
                redis.call('ZREM', orderQueueKey, unpack(items))
            end
        end
        
        -- 3. OTHER retry (eligible만)
        if otherRetrySlots > 0 then
            local items = redis.call('ZRANGEBYSCORE', otherRetryKey, 0, retryThreshold, 'LIMIT', 0, otherRetrySlots)
            if #items > 0 then
                for _, item in ipairs(items) do
                    table.insert(result.otherRetry, item)
                end
                redis.call('ZREM', otherRetryKey, unpack(items))
            end
            otherNormalSlots = otherNormalSlots + (otherRetrySlots - #items)
        end
        
        -- 4. OTHER normal
        if otherNormalSlots > 0 then
            local items = redis.call('ZRANGE', otherQueueKey, 0, otherNormalSlots - 1)
            if #items > 0 then
                for _, item in ipairs(items) do
                    table.insert(result.otherNormal, item)
                end
                redis.call('ZREM', otherQueueKey, unpack(items))
            end
        end
        
        return cjson.encode(result)
        """;

    private RedisScript<String> weightedPollScript;

    @PostConstruct
    public void init() {
        weightedPollScript = RedisScript.of(WEIGHTED_POLL_LUA, String.class);
        initMetrics();
    }

    private void initMetrics() {
        // 대기열 대기 시간 (히스토그램)
        queueWaitTimer = Timer.builder("queue.wait.time")
                .description("Time spent waiting in queue before processing")
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(meterRegistry);

        // 첫 시도 성공
        firstAttemptSuccessCounter = Counter.builder("queue.process.first_attempt.success")
                .description("Number of requests succeeded on first attempt")
                .register(meterRegistry);

        // 재시도 성공 (태그로 재시도 횟수 구분)
        retrySuccessCounter = Counter.builder("queue.process.retry.success")
                .description("Number of requests succeeded after retry")
                .register(meterRegistry);

        // 재시도 실패 (DLQ 이동)
        retryFailCounter = Counter.builder("queue.process.retry.fail")
                .description("Number of requests failed after retry (moved to DLQ)")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${queue.processor-interval-ms:100}")
    public void processQueue() {
        globalQueueService.getTotalQueueSize()
                .zipWith(globalQueueService.getTotalRetryQueueSize())
                .filter(sizes -> sizes.getT1() > 0 || sizes.getT2() > 0)
                .flatMap(sizes ->
                        pgRateLimiterService.getAvailableTokens(DEFAULT_PROVIDER)
                                .filter(pgTokens -> pgTokens > 0)
                                .flatMap(pgTokens -> globalRateLimiterService.getAvailableTokens()
                                        .map(globalTokens -> Math.min(pgTokens.intValue(), globalTokens.intValue())))
                                .filter(availableSlots -> availableSlots > 0)
                                .flatMap(this::processWithWeightedLua)
                )
                .subscribe(null, e -> log.error("Queue processing error", e));
    }

    private Mono<Void> processWithWeightedLua(int availableSlots) {
        QueueProperties.Weight weight = queueProperties.getWeight();
        long retryThreshold = System.currentTimeMillis() -
                (queueProperties.getRetry().getEligibleAfterSeconds() * 1000L);

        return reactiveRedisTemplate.execute(
                        weightedPollScript,
                        Arrays.asList(ORDER_QUEUE_KEY, OTHER_QUEUE_KEY, ORDER_RETRY_KEY, OTHER_RETRY_KEY),
                        String.valueOf(availableSlots),
                        String.valueOf(weight.getOrder()),
                        String.valueOf(weight.getOther()),
                        String.valueOf(weight.getRetryRatio()),
                        String.valueOf(retryThreshold)
                )
                .next()
                .flatMap(result -> {
                    try {
                        Map<String, List<String>> parsed = objectMapper.readValue(
                                result, new TypeReference<Map<String, List<String>>>() {});

                        List<String> orderRetry = parsed.getOrDefault("orderRetry", List.of());
                        List<String> orderNormal = parsed.getOrDefault("orderNormal", List.of());
                        List<String> otherRetry = parsed.getOrDefault("otherRetry", List.of());
                        List<String> otherNormal = parsed.getOrDefault("otherNormal", List.of());

                        log.debug("Processing - ORDER: retry={}, normal={} | OTHER: retry={}, normal={}",
                                orderRetry.size(), orderNormal.size(), otherRetry.size(), otherNormal.size());

                        return processItems(orderRetry, QueueType.ORDER, true)
                                .then(processItems(orderNormal, QueueType.ORDER, false))
                                .then(processItems(otherRetry, QueueType.OTHER, true))
                                .then(processItems(otherNormal, QueueType.OTHER, false));

                    } catch (Exception e) {
                        log.error("Failed to parse weighted poll result", e);
                        return Mono.empty();
                    }
                });
    }

    private Mono<Void> processItems(List<String> items, QueueType queueType, boolean isRetry) {
        if (items == null || items.isEmpty()) {
            return Mono.empty();
        }

        return Flux.fromIterable(items)
                .map(this::deserialize)
                .flatMap(item -> processItem(item, queueType, isRetry), 1)
                .then();
    }

    private Mono<Void> processItem(QueueItem item, QueueType queueType, boolean isRetry) {
        // 대기 시간 계산 (큐에서 꺼내지기까지)
        long waitTimeMs = System.currentTimeMillis() - item.getOriginalTimestamp();

        return globalRateLimiterService.tryConsumeForQueueRequest()
                .flatMap(globalOk -> {
                    if (!globalOk) {
                        log.debug("Global token exhausted, re-queuing userId={}", item.getUserId());
                        return requeue(item, queueType, isRetry);
                    }
                    return pgRateLimiterService.tryConsume(DEFAULT_PROVIDER)
                            .flatMap(pgOk -> {
                                if (!pgOk) {
                                    log.debug("PG token exhausted, re-queuing userId={}", item.getUserId());
                                    return requeue(item, queueType, isRetry);
                                }
                                return executeRequest(item, queueType, isRetry, waitTimeMs);
                            });
                });
    }

    private Mono<Void> requeue(QueueItem item, QueueType queueType, boolean wasRetry) {
        if (wasRetry) {
            return globalQueueService.offerToRetry(item, queueType).then();
        }
        return globalQueueService.offer(item, queueType)
                .onErrorResume(e -> {
                    log.error("Queue full, moving to DLQ userId={}", item.getUserId());
                    return globalQueueService.offerToDlq(item, "QUEUE_FULL");
                })
                .then();
    }

    private Mono<Void> executeRequest(QueueItem item, QueueType queueType, boolean isRetry, long waitTimeMs) {
        String command = item.getCommand();

        if ("CREATE_ORDER".equals(command)) {
            return executeOrderRequest(item, queueType, isRetry, waitTimeMs);
        }

        log.warn("Unknown command: {}, userId={}", command, item.getUserId());
        return Mono.empty();
    }

    private String extractPath(String uri) {
        try {
            URI parsed = new URI(uri);
            String path = parsed.getPath();
            String query = parsed.getQuery();
            return query != null ? path + "?" + query : path;
        } catch (Exception e) {
            return uri;
        }
    }

    private Mono<Void> executeOrderRequest(QueueItem item, QueueType queueType, boolean isRetry, long waitTimeMs) {
        HttpRequestData request = item.getHttpRequest();
        if (request == null) {
            log.warn("Invalid request data for userId={}", item.getUserId());
            return Mono.empty();
        }

        URI original = URI.create(request.getUri());

        return webClient
                .method(HttpMethod.valueOf(request.getMethod()))
                .uri(uriBuilder -> uriBuilder
                        .scheme("lb")
                        .host("order-to-shipping-service")
                        .path(original.getPath())
                        .query(original.getQuery())
                        .queryParam("provider", DEFAULT_PROVIDER)
                        .build(true))
                .headers(headers -> {
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    if (request.getHeaders() != null) {
                        request.getHeaders().forEach(headers::add);
                    }
                    if (item.getAccessToken() != null) {
                        headers.set("Authorization", "Bearer " + item.getAccessToken());
                    }
                })
                .body(request.getBody() != null ? BodyInserters.fromValue(request.getBody()) : BodyInserters.empty())
                .retrieve()
                .onStatus(status -> status.is5xxServerError(),
                        response -> Mono.error(new RuntimeException("Server error: " + response.statusCode())))
                .bodyToMono(String.class)
                .timeout(REQUEST_TIMEOUT)
                .doOnSuccess(r -> {
                    // 성공 메트릭 기록
                    recordSuccessMetrics(item, isRetry, waitTimeMs);
                    log.info("Processed order for userId={}, isRetry={}, retryCount={}, waitTime={}ms",
                            item.getUserId(), isRetry, item.getRetryCount(), waitTimeMs);
                })
                .then()
                .onErrorResume(e -> handleRequestError(e, item, queueType, isRetry, waitTimeMs));
    }

    /**
     * 성공 메트릭 기록
     */
    private void recordSuccessMetrics(QueueItem item, boolean isRetry, long waitTimeMs) {
        // 대기 시간 기록
        queueWaitTimer.record(waitTimeMs, TimeUnit.MILLISECONDS);

        if (isRetry) {
            // 재시도 성공 (몇 번째 재시도인지 태그로)
            Counter.builder("queue.process.retry.success")
                    .tag("retry_count", String.valueOf(item.getRetryCount()))
                    .tag("queue_type", item.getCommand())
                    .description("Retry success count by attempt number")
                    .register(meterRegistry)
                    .increment();

            log.info("Retry SUCCESS - userId={}, retryCount={}, totalWaitTime={}ms",
                    item.getUserId(), item.getRetryCount(), waitTimeMs);
        } else {
            // 첫 시도 성공
            Counter.builder("queue.process.first_attempt.success")
                    .tag("queue_type", item.getCommand())
                    .register(meterRegistry)
                    .increment();
        }
    }

    private Mono<Void> handleRequestError(Throwable e, QueueItem item, QueueType queueType,
                                          boolean isRetry, long waitTimeMs) {
        log.error("Request failed for userId={}, isRetry={}, retryCount={}, waitTime={}ms, error={}",
                item.getUserId(), isRetry, item.getRetryCount(), waitTimeMs, e.getMessage());

        // 이미 재시도한 건 DLQ로
        if (isRetry) {
            // 재시도 실패 메트릭
            Counter.builder("queue.process.retry.fail")
                    .tag("retry_count", String.valueOf(item.getRetryCount()))
                    .tag("queue_type", item.getCommand())
                    .tag("error_type", classifyError(e))
                    .description("Retry failure count")
                    .register(meterRegistry)
                    .increment();

            log.error("Retry FAILED - userId={}, retryCount={}, totalWaitTime={}ms, moving to DLQ",
                    item.getUserId(), item.getRetryCount(), waitTimeMs);

            return globalQueueService.offerToDlq(item, e.getMessage()).then();
        }

        // 재시도 가능한 에러인지 확인
        if (isRetryable(e) && item.canRetry()) {
            item.incrementRetryCount();
            log.warn("Retryable error, moving to retry queue userId={}, willRetryCount={}",
                    item.getUserId(), item.getRetryCount());
            return globalQueueService.offerToRetry(item, queueType).then();
        }

        // 재시도 불가능한 에러 메트릭
        Counter.builder("queue.process.non_retryable.fail")
                .tag("queue_type", item.getCommand())
                .tag("error_type", classifyError(e))
                .description("Non-retryable failure count")
                .register(meterRegistry)
                .increment();

        log.error("Non-retryable error, moving to DLQ userId={}", item.getUserId());
        return globalQueueService.offerToDlq(item, e.getMessage()).then();
    }

    /**
     * 에러 타입 분류
     */
    private String classifyError(Throwable e) {
        if (e instanceof TimeoutException) return "TIMEOUT";
        if (e instanceof ConnectException) return "CONNECT";
        if (e instanceof ConnectTimeoutException) return "CONNECT_TIMEOUT";
        if (e instanceof WebClientRequestException) return "WEBCLIENT";
        if (e instanceof RuntimeException && e.getMessage() != null
                && e.getMessage().contains("Server error")) return "SERVER_5XX";
        return "UNKNOWN";
    }

    private boolean isRetryable(Throwable e) {
        if (e instanceof TimeoutException) return true;
        if (e instanceof ConnectException) return true;
        if (e instanceof ConnectTimeoutException) return true;
        if (e instanceof WebClientRequestException) return true;
        if (e instanceof RuntimeException && e.getMessage() != null
                && e.getMessage().contains("Server error")) return true;

        if (e.getCause() != null) {
            return isRetryable(e.getCause());
        }
        return false;
    }

    private QueueItem deserialize(String json) {
        try {
            return objectMapper.readValue(json, QueueItem.class);
        } catch (Exception e) {
            log.error("Failed to deserialize QueueItem: {}", json, e);
            throw new RuntimeException(e);
        }
    }

    private record QueueOrderRequest(Long userId, java.util.UUID resourceId, String idempotencyKey) {}
}
