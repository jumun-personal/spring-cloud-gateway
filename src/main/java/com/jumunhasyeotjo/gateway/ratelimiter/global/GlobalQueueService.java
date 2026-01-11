package com.jumunhasyeotjo.gateway.ratelimiter.global;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
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
    private static final String GLOBAL_QUEUE_KEY = "order:queue:global";

    public Mono<Boolean> offer(QueueItem item) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(item))
                .flatMap(value -> {
                    double score = System.currentTimeMillis();
                    return reactiveRedisTemplate.opsForZSet().add(GLOBAL_QUEUE_KEY, value, score);
                });
    }

    public Mono<List<QueueItem>> poll(int size) {
        return reactiveRedisTemplate.opsForZSet()
                .range(GLOBAL_QUEUE_KEY, Range.closed(0L, (long) size - 1))
                .collectList()
                .flatMap(items -> {
                    if (items.isEmpty()) {
                        return Mono.just(Collections.emptyList());
                    }

                    List<QueueItem> result = items.stream()
                            .map(this::deserialize)
                            .collect(Collectors.toList());

                    return reactiveRedisTemplate.opsForZSet()
                            .remove(GLOBAL_QUEUE_KEY, items.toArray())
                            .thenReturn(result);
                });
    }

    public Mono<Long> findSequence(Long userId) {
        return reactiveRedisTemplate.opsForZSet()
                .range(GLOBAL_QUEUE_KEY, Range.closed(0L, Long.MAX_VALUE))
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
                .size(GLOBAL_QUEUE_KEY)
                .defaultIfEmpty(0L);
    }

    private QueueItem deserialize(String json) {
        log.info("QueueItem deserialize {} ", json);
        try {
            return objectMapper.readValue(json, QueueItem.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
