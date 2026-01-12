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
        pgRateLimiterService.getAvailableTokens(DEFAULT_PROVIDER)
                .filter(pgTokens -> pgTokens > 0)
                .flatMap(pgTokens -> globalRateLimiterService.getAvailableTokens()
                        .map(globalTokens -> Math.min(pgTokens.intValue(), globalTokens.intValue())))
                .filter(availableSlots -> availableSlots > 0)
                .flatMap(this::processWithDynamicWeight)
                .subscribe(null, e -> log.error("Queue processing error", e));
    }

    private Mono<Void> processWithDynamicWeight(int availableSlots) {
        return globalQueueService.getQueueSize(QueueType.ORDER)
                .zipWith(globalQueueService.getQueueSize(QueueType.OTHER))
                .flatMap(sizes -> {
                    int orderQueueSize = sizes.getT1().intValue();
                    int otherQueueSize = sizes.getT2().intValue();

                    int[] slots = calculateSlots(availableSlots, orderQueueSize, otherQueueSize);
                    int orderSlots = slots[0];
                    int otherSlots = slots[1];

                    log.debug("Processing - available: {}, orderQ: {}, otherQ: {}, orderSlots: {}, otherSlots: {}",
                            availableSlots, orderQueueSize, otherQueueSize, orderSlots, otherSlots);

                    return processQueueByType(QueueType.ORDER, orderSlots)
                            .then(processQueueByType(QueueType.OTHER, otherSlots));
                });
    }

    /**
     * 동적 슬롯 분배
     */
    private int[] calculateSlots(int availableSlots, int orderQueueSize, int otherQueueSize) {
        if (orderQueueSize == 0 && otherQueueSize == 0) {
            return new int[]{0, 0};
        }
        if (orderQueueSize == 0) {
            return new int[]{0, Math.min(availableSlots, otherQueueSize)};
        }
        if (otherQueueSize == 0) {
            return new int[]{Math.min(availableSlots, orderQueueSize), 0};
        }

        // 가중치 기반 기본 할당
        int orderWeight = weightProperties.getOrder();
        int otherWeight = weightProperties.getOther();
        int totalWeight = orderWeight + otherWeight;

        int baseOrderSlots = (int) Math.ceil(availableSlots * (double) orderWeight / totalWeight);
        int baseOtherSlots = availableSlots - baseOrderSlots;

        // 큐 크기에 맞게 조정 (큐에 있는 것보다 더 많이 가져갈 수 없음)
        int actualOrderSlots = Math.min(baseOrderSlots, orderQueueSize);
        int actualOtherSlots = Math.min(baseOtherSlots, otherQueueSize);

        // 남는 슬롯 재분배
        int remainingSlots = availableSlots - actualOrderSlots - actualOtherSlots;

        if (remainingSlots > 0) {
            int orderCanTakeMore = orderQueueSize - actualOrderSlots;
            int otherCanTakeMore = otherQueueSize - actualOtherSlots;

            if (orderCanTakeMore > 0) {
                int extra = Math.min(remainingSlots, orderCanTakeMore);
                actualOrderSlots += extra;
                remainingSlots -= extra;
            }
            if (remainingSlots > 0 && otherCanTakeMore > 0) {
                int extra = Math.min(remainingSlots, otherCanTakeMore);
                actualOtherSlots += extra;
            }
        }

        return new int[]{actualOrderSlots, actualOtherSlots};
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
