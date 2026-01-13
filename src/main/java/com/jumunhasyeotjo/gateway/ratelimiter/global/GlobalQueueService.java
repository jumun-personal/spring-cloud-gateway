package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class GlobalQueueService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final ObjectMapper objectMapper;
    private final QueueProperties queueProperties;

    private static final String ORDER_QUEUE_KEY = "queue:global:order";
    private static final String OTHER_QUEUE_KEY = "queue:global:other";
    private static final String ORDER_RETRY_QUEUE_KEY = "queue:global:order:retry";
    private static final String OTHER_RETRY_QUEUE_KEY = "queue:global:other:retry";
    private static final String DLQ_KEY = "queue:global:dlq";

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

    public QueueType resolveQueueType(String command) {
        if ("CREATE_ORDER".equals(command)) {
            return QueueType.ORDER;
        }
        return QueueType.OTHER;
    }

    /**
     * 대기열에 추가 (크기 제한 체크)
     */
    public Mono<Boolean> offer(QueueItem item, QueueType queueType) {
        return getTotalQueueSize()
                .flatMap(currentSize -> {
                    if (currentSize >= queueProperties.getMaxSize()) {
                        log.warn("Queue full, rejecting request. current={}, max={}", 
                                currentSize, queueProperties.getMaxSize());
                        return Mono.error(new ResponseStatusException(
                                HttpStatus.SERVICE_UNAVAILABLE, 
                                "대기열이 가득 찼습니다. 잠시 후 다시 시도해주세요."));
                    }
                    return doOffer(item, queueType.getKey());
                });
    }

    /**
     * 재시도 큐에 추가
     */
    public Mono<Boolean> offerToRetry(QueueItem item, QueueType queueType) {
        return doOffer(item, queueType.getRetryKey())
                .doOnNext(added -> log.debug("Retry queue offer [{}]: {}", queueType, added));
    }

    /**
     * DLQ에 추가 (재시도 실패 시)
     */
    public Mono<Boolean> offerToDlq(QueueItem item, String failReason) {
        return Mono.fromCallable(() -> {
                    item.setCommand(item.getCommand() + ":FAILED:" + failReason);
                    return objectMapper.writeValueAsString(item);
                })
                .flatMap(value -> {
                    double score = System.currentTimeMillis();
                    return reactiveRedisTemplate.opsForZSet().add(DLQ_KEY, value, score);
                })
                .doOnNext(added -> log.error("DLQ offer: userId={}, reason={}", item.getUserId(), failReason));
    }

    private Mono<Boolean> doOffer(QueueItem item, String key) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(item))
                .flatMap(value -> {
                    double score = item.getOriginalTimestamp();
                    return reactiveRedisTemplate.opsForZSet().add(key, value, score);
                });
    }

    public Mono<List<QueueItem>> poll(QueueType queueType, int size) {
        return pollFromKey(queueType.getKey(), size);
    }

    /**
     * 재시도 큐에서 eligible 아이템만 조회 (설정된 시간 경과한 것)
     */
    public Mono<List<QueueItem>> pollRetryEligible(QueueType queueType, int size) {
        long thresholdMs = queueProperties.getRetry().getEligibleAfterSeconds() * 1000L;
        long threshold = System.currentTimeMillis() - thresholdMs;

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
     * eligible 재시도 아이템 수 (설정된 시간 경과한 것)
     */
    public Mono<Long> getRetryEligibleCount(QueueType queueType) {
        long thresholdMs = queueProperties.getRetry().getEligibleAfterSeconds() * 1000L;
        long threshold = System.currentTimeMillis() - thresholdMs;
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

    public Mono<Long> getDlqSize() {
        return reactiveRedisTemplate.opsForZSet()
                .size(DLQ_KEY)
                .defaultIfEmpty(0L);
    }

    private QueueItem deserialize(String json) {
        try {
            return objectMapper.readValue(json, QueueItem.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize QueueItem: {}", json, e);
            throw new RuntimeException(e);
        }
    }
}
