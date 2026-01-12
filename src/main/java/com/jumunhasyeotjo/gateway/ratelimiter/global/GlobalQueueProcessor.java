package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.jumunhasyeotjo.gateway.ratelimiter.HttpRequestData;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalQueueService.QueueType;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.ReactiveRateLimiterService;
import io.netty.channel.ConnectTimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import java.util.concurrent.TimeoutException;

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
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);

    @Scheduled(fixedDelay = 100)
    public void processQueue() {
        pgRateLimiterService.getAvailableTokens(DEFAULT_PROVIDER)
                .filter(pgTokens -> pgTokens > 0)
                .flatMap(pgTokens -> globalRateLimiterService.getAvailableTokens()
                        .map(globalTokens -> Math.min(pgTokens.intValue(), globalTokens.intValue())))
                .filter(availableSlots -> availableSlots > 0)
                .flatMap(this::processQueues)
                .subscribe(null, e -> log.error("Queue processing error", e));
    }

    private Mono<Void> processQueues(int availableSlots) {
        // 1. 재시도 큐 먼저 처리 (4초 경과한 것만)
        return processRetryQueues(availableSlots)
                .flatMap(remainingSlots -> {
                    if (remainingSlots <= 0) {
                        return Mono.empty();
                    }
                    // 2. 일반 큐 처리
                    return processNormalQueues(remainingSlots);
                });
    }

    /**
     * 재시도 큐 처리 - 4초 이상 경과한 아이템만
     * @return 남은 슬롯 수
     */
    private Mono<Integer> processRetryQueues(int availableSlots) {
        return globalQueueService.getRetryEligibleCount(QueueType.ORDER)
                .zipWith(globalQueueService.getRetryEligibleCount(QueueType.OTHER))
                .flatMap(counts -> {
                    int orderRetryCount = counts.getT1().intValue();
                    int otherRetryCount = counts.getT2().intValue();
                    int totalRetry = orderRetryCount + otherRetryCount;

                    if (totalRetry == 0) {
                        return Mono.just(availableSlots);
                    }

                    // 재시도 큐에서 가져올 수 있는 만큼 처리
                    int retrySlots = Math.min(availableSlots, totalRetry);
                    int orderRetrySlots = Math.min(orderRetryCount, retrySlots);
                    int otherRetrySlots = Math.min(otherRetryCount, retrySlots - orderRetrySlots);

                    log.info("Processing retry queues - orderRetry: {}, otherRetry: {}", 
                            orderRetrySlots, otherRetrySlots);

                    return processRetryQueueByType(QueueType.ORDER, orderRetrySlots)
                            .then(processRetryQueueByType(QueueType.OTHER, otherRetrySlots))
                            .thenReturn(availableSlots - orderRetrySlots - otherRetrySlots);
                });
    }

    private Mono<Void> processRetryQueueByType(QueueType queueType, int count) {
        if (count <= 0) {
            return Mono.empty();
        }

        return globalQueueService.pollRetryEligible(queueType, count)
                .flatMapMany(Flux::fromIterable)
                .flatMap(item -> processRetryItem(item, queueType), 1)
                .then();
    }

    private Mono<Void> processRetryItem(QueueItem item, QueueType queueType) {
        return globalRateLimiterService.tryConsume()
                .flatMap(globalOk -> {
                    if (!globalOk) {
                        log.debug("Global token exhausted for retry, re-queuing userId={}", item.getUserId());
                        return globalQueueService.offerToRetry(item, queueType).then();
                    }
                    return pgRateLimiterService.tryConsume(DEFAULT_PROVIDER)
                            .flatMap(pgOk -> {
                                if (!pgOk) {
                                    log.debug("PG token exhausted for retry, re-queuing userId={}", item.getUserId());
                                    return globalQueueService.offerToRetry(item, queueType).then();
                                }
                                return executeRequest(item, queueType, true);
                            });
                });
    }

    /**
     * 일반 큐 처리
     */
    private Mono<Void> processNormalQueues(int availableSlots) {
        return globalQueueService.getQueueSize(QueueType.ORDER)
                .zipWith(globalQueueService.getQueueSize(QueueType.OTHER))
                .flatMap(sizes -> {
                    int orderQueueSize = sizes.getT1().intValue();
                    int otherQueueSize = sizes.getT2().intValue();

                    int[] slots = calculateSlots(availableSlots, orderQueueSize, otherQueueSize);
                    int orderSlots = slots[0];
                    int otherSlots = slots[1];

                    log.debug("Processing normal - available: {}, orderQ: {}, otherQ: {}, orderSlots: {}, otherSlots: {}",
                            availableSlots, orderQueueSize, otherQueueSize, orderSlots, otherSlots);

                    return processQueueByType(QueueType.ORDER, orderSlots)
                            .then(processQueueByType(QueueType.OTHER, otherSlots));
                });
    }

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

        int orderWeight = weightProperties.getOrder();
        int otherWeight = weightProperties.getOther();
        int totalWeight = orderWeight + otherWeight;

        int baseOrderSlots = (int) Math.ceil(availableSlots * (double) orderWeight / totalWeight);
        int baseOtherSlots = availableSlots - baseOrderSlots;

        int actualOrderSlots = Math.min(baseOrderSlots, orderQueueSize);
        int actualOtherSlots = Math.min(baseOtherSlots, otherQueueSize);

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
                                return executeRequest(item, queueType, false);
                            });
                });
    }

    private Mono<Void> executeRequest(QueueItem item, QueueType queueType, boolean isRetry) {
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
                .onStatus(status -> status.is5xxServerError(), 
                        response -> Mono.error(new RuntimeException("Server error: " + response.statusCode())))
                .bodyToMono(String.class)
                .timeout(REQUEST_TIMEOUT)
                .doOnSuccess(r -> log.info("Processed {} request for userId={}, isRetry={}", 
                        DEFAULT_PROVIDER, item.getUserId(), isRetry))
                .then()
                .onErrorResume(e -> handleRequestError(e, item, queueType, isRetry));
    }

    private Mono<Void> handleRequestError(Throwable e, QueueItem item, QueueType queueType, boolean isRetry) {
        log.error("Request failed for userId={}, isRetry={}, error={}", 
                item.getUserId(), isRetry, e.getMessage());

        // 이미 재시도한 건 폐기
        if (isRetry) {
            log.error("Retry also failed, dropping request for userId={}", item.getUserId());
            return Mono.empty();
        }

        // 재시도 가능한 에러인지 확인
        if (isRetryable(e) && item.canRetry()) {
            item.incrementRetryCount();
            log.warn("Retryable error, moving to retry queue userId={}", item.getUserId());
            return globalQueueService.offerToRetry(item, queueType).then();
        }

        log.error("Non-retryable error or max retry exceeded, dropping userId={}", item.getUserId());
        return Mono.empty();
    }

    private boolean isRetryable(Throwable e) {
        if (e instanceof TimeoutException) return true;
        if (e instanceof ConnectException) return true;
        if (e instanceof ConnectTimeoutException) return true;
        if (e instanceof WebClientRequestException) return true;
        // 5xx 서버 에러 (RuntimeException으로 래핑됨)
        if (e instanceof RuntimeException && e.getMessage() != null 
                && e.getMessage().contains("Server error")) return true;
        
        if (e.getCause() != null) {
            return isRetryable(e.getCause());
        }
        return false;
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
