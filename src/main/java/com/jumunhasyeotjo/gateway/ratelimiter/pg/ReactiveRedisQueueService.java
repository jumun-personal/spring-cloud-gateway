package com.jumunhasyeotjo.gateway.ratelimiter.pg;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ReactiveRedisQueueService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final ObjectMapper objectMapper;
    private static final String QUEUE_KEY = "order:queue"; // 단일 대기열

    public Mono<Boolean> offer(QueueItem item) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(item))
                .flatMap(value -> {
                    double score = System.currentTimeMillis();
                    return reactiveRedisTemplate.opsForZSet().add(QUEUE_KEY, value, score);
                });
    }

    public Mono<List<QueueItem>> poll(int size) {
        return reactiveRedisTemplate.opsForZSet()
                .range(QUEUE_KEY, Range.closed(0L, (long) size - 1))
                .collectList()
                .flatMap(items -> {
                    if (items.isEmpty()) {
                        return Mono.just(Collections.emptyList());
                    }

                    List<QueueItem> result = items.stream()
                            .map(this::deserialize)
                            .collect(Collectors.toList());

                    return reactiveRedisTemplate.opsForZSet()
                            .remove(QUEUE_KEY, items.toArray())
                            .thenReturn(result);
                });
    }

    public Mono<Long> findSequence(Long userId) {
        return reactiveRedisTemplate.opsForZSet()
                .range(QUEUE_KEY, Range.closed(0L, Long.MAX_VALUE))
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

    public Mono<Long> getQueueSize() {
        return reactiveRedisTemplate.opsForZSet()
                .size(QUEUE_KEY)
                .defaultIfEmpty(0L);
    }

    private QueueItem deserialize(String json) {
        try {
            return objectMapper.readValue(json, QueueItem.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}