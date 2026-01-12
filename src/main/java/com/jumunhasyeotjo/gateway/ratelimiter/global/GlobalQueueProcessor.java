package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.jumunhasyeotjo.gateway.ratelimiter.HttpRequestData;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalQueueService.QueueType;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.ReactiveRateLimiterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;

@Slf4j
@Component
@RequiredArgsConstructor
public class GlobalQueueProcessor {

    private final GlobalQueueService globalQueueService;
    private final GlobalRateLimiterService globalRateLimiterService;
    private final ReactiveRateLimiterService pgRateLimiterService;
    private final QueueWeightProperties weightProperties;
    private final WebClient webClient;

    private static final String DEFAULT_PROVIDER = "TOSS";

    @Scheduled(fixedDelay = 100)
    public void processQueue() {
        // 1. PG 가용 토큰 확인
        pgRateLimiterService.getAvailableTokens(DEFAULT_PROVIDER)
                .filter(pgTokens -> pgTokens > 0)
                .flatMap(pgTokens -> {
                    // 2. Global 가용 토큰 확인
                    return globalRateLimiterService.getAvailableTokens()
                            .map(globalTokens -> Math.min(pgTokens.intValue(), globalTokens.intValue()));
                })
                .filter(availableSlots -> availableSlots > 0)
                .flatMap(availableSlots -> {
                    // 3. 가중치 비율로 분배
                    int orderSlots = (int) Math.ceil(availableSlots * weightProperties.getOrderRatio());
                    int otherSlots = availableSlots - orderSlots;

                    log.debug("Processing slots - total: {}, order: {}, other: {}", 
                            availableSlots, orderSlots, otherSlots);

                    // 4. 각 큐 처리
                    return processQueueByType(QueueType.ORDER, orderSlots)
                            .then(processQueueByType(QueueType.OTHER, otherSlots));
                })
                .subscribe(
                        null,
                        e -> log.error("Queue processing error", e)
                );
    }

    private Mono<Void> processQueueByType(QueueType queueType, int count) {
        if (count <= 0) {
            return Mono.empty();
        }

        return globalQueueService.poll(queueType, count)
                .flatMapMany(Flux::fromIterable)
                .flatMap(item -> processItem(item, queueType), 1)
                .then();
    }

    private Mono<Void> processItem(QueueItem item, QueueType queueType) {
        // Global 토큰 소비 → PG 토큰 소비 → 요청 실행
        return globalRateLimiterService.tryConsume()
                .flatMap(globalOk -> {
                    if (!globalOk) {
                        log.debug("Global token exhausted, re-queuing userId={}", item.getUserId());
                        return globalQueueService.offer(item, queueType).then();
                    }
                    return pgRateLimiterService.tryConsume(DEFAULT_PROVIDER)
                            .flatMap(pgOk -> {
                                if (!pgOk) {
                                    log.debug("PG token exhausted, re-queuing userId={}", item.getUserId());
                                    return globalQueueService.offer(item, queueType).then();
                                }
                                return executeRequest(item);
                            });
                });
    }

    private Mono<Void> executeRequest(QueueItem item) {
        HttpRequestData request = item.getHttpRequest();
        if (request == null) {
            log.warn("Invalid request data for userId={}", item.getUserId());
            return Mono.empty();
        }

        String path = extractPath(request.getUri());

        return webClient
                .method(HttpMethod.valueOf(request.getMethod()))
                .uri(uriBuilder -> uriBuilder
                        .scheme("lb")
                        .host("order-to-shipping-service")
                        .path(path)
                        .queryParam("provider", DEFAULT_PROVIDER)
                        .build())
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
                .bodyToMono(String.class)
                .doOnSuccess(r -> log.info("Processed {} request for userId={}", DEFAULT_PROVIDER, item.getUserId()))
                .doOnError(e -> log.error("Request failed for userId={}: {}", item.getUserId(), e.getMessage()))
                .then()
                .onErrorResume(e -> Mono.empty());
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
}
