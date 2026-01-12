package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
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

    private static final String ORDER_QUEUE_KEY = "queue:global:order";
    private static final String OTHER_QUEUE_KEY = "queue:global:other";

    @Getter
    public enum QueueType {
        ORDER(ORDER_QUEUE_KEY),
        OTHER(OTHER_QUEUE_KEY);

        private final String key;

        QueueType(String key) {
            this.key = key;
        }
    }

    public QueueType resolveQueueType(String uri) {
        if (uri != null && uri.contains("/orders")) {
            return QueueType.ORDER;
        }
        return QueueType.OTHER;
    }

    public Mono<Boolean> offer(QueueItem item, QueueType queueType) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(item))
                .flatMap(value -> {
                    double score = System.currentTimeMillis();
                    return reactiveRedisTemplate.opsForZSet().add(queueType.getKey(), value, score);
                })
                .doOnNext(added -> log.debug("Queue offer [{}]: {}", queueType, added));
    }

    public Mono<Boolean> offer(QueueItem item) {
        String uri = item.getHttpRequest() != null ? item.getHttpRequest().getUri() : null;
        QueueType queueType = resolveQueueType(uri);
        return offer(item, queueType);
    }

    public Mono<List<QueueItem>> poll(QueueType queueType, int size) {
        if (size <= 0) {
            return Mono.just(Collections.emptyList());
        }
        
        return reactiveRedisTemplate.opsForZSet()
                .range(queueType.getKey(), Range.closed(0L, (long) size - 1))
                .collectList()
                .flatMap(items -> {
                    if (items.isEmpty()) {
                        return Mono.just(Collections.emptyList());
                    }

                    List<QueueItem> result = items.stream()
                            .map(this::deserialize)
                            .collect(Collectors.toList());

                    return reactiveRedisTemplate.opsForZSet()
                            .remove(queueType.getKey(), items.toArray())
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

    public Mono<Long> getTotalQueueSize() {
        return getQueueSize(QueueType.ORDER)
                .zipWith(getQueueSize(QueueType.OTHER))
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
}
