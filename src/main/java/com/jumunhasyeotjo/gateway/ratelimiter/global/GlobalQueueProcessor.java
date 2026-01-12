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
                .flatMap(this::processWithDynamicWeight)
                .subscribe(null, e -> log.error("Queue processing error", e));
    }

    private Mono<Void> processWithDynamicWeight(int availableSlots) {
        // 1. 모든 큐 사이즈 조회
        return Mono.zip(
                globalQueueService.getQueueSize(QueueType.ORDER),
                globalQueueService.getQueueSize(QueueType.OTHER),
                globalQueueService.getRetryEligibleCount(QueueType.ORDER),
                globalQueueService.getRetryEligibleCount(QueueType.OTHER)
        ).flatMap(tuple -> {
            int orderQueueSize = tuple.getT1().intValue();
            int otherQueueSize = tuple.getT2().intValue();
            int orderRetrySize = tuple.getT3().intValue();
            int otherRetrySize = tuple.getT4().intValue();

            // 2. 가중치로 ORDER/OTHER 슬롯 분배
            int[] slots = calculateSlots(availableSlots, 
                    orderQueueSize + orderRetrySize, 
                    otherQueueSize + otherRetrySize);
            int orderSlots = slots[0];
            int otherSlots = slots[1];

            // 3. 각 타입 내에서 retry 우선 처리
            int orderRetrySlots = Math.min(orderSlots, orderRetrySize);
            int orderNormalSlots = orderSlots - orderRetrySlots;

            int otherRetrySlots = Math.min(otherSlots, otherRetrySize);
            int otherNormalSlots = otherSlots - otherRetrySlots;

            log.debug("Processing - ORDER: retry={}, normal={} | OTHER: retry={}, normal={}",
                    orderRetrySlots, orderNormalSlots, otherRetrySlots, otherNormalSlots);

            // 4. 처리 실행
            return processQueueType(QueueType.ORDER, orderRetrySlots, orderNormalSlots)
                    .then(processQueueType(QueueType.OTHER, otherRetrySlots, otherNormalSlots));
        });
    }

    private Mono<Void> processQueueType(QueueType queueType, int retrySlots, int normalSlots) {
        return processRetryQueue(queueType, retrySlots)
                .then(processNormalQueue(queueType, normalSlots));
    }

    private Mono<Void> processRetryQueue(QueueType queueType, int count) {
        if (count <= 0) {
            return Mono.empty();
        }

        return globalQueueService.pollRetryEligible(queueType, count)
                .flatMapMany(Flux::fromIterable)
                .flatMap(item -> processItem(item, queueType, true), 1)
                .then();
    }

    private Mono<Void> processNormalQueue(QueueType queueType, int count) {
        if (count <= 0) {
            return Mono.empty();
        }

        return globalQueueService.poll(queueType, count)
                .flatMapMany(Flux::fromIterable)
                .flatMap(item -> processItem(item, queueType, false), 1)
                .then();
    }

    private int[] calculateSlots(int availableSlots, int orderTotal, int otherTotal) {
        if (orderTotal == 0 && otherTotal == 0) {
            return new int[]{0, 0};
        }
        if (orderTotal == 0) {
            return new int[]{0, Math.min(availableSlots, otherTotal)};
        }
        if (otherTotal == 0) {
            return new int[]{Math.min(availableSlots, orderTotal), 0};
        }

        int orderWeight = weightProperties.getOrder();
        int otherWeight = weightProperties.getOther();
        int totalWeight = orderWeight + otherWeight;

        int baseOrderSlots = (int) Math.ceil(availableSlots * (double) orderWeight / totalWeight);
        int baseOtherSlots = availableSlots - baseOrderSlots;

        int actualOrderSlots = Math.min(baseOrderSlots, orderTotal);
        int actualOtherSlots = Math.min(baseOtherSlots, otherTotal);

        int remainingSlots = availableSlots - actualOrderSlots - actualOtherSlots;

        if (remainingSlots > 0) {
            int orderCanTakeMore = orderTotal - actualOrderSlots;
            int otherCanTakeMore = otherTotal - actualOtherSlots;

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

    private Mono<Void> processItem(QueueItem item, QueueType queueType, boolean isRetry) {
        return globalRateLimiterService.tryConsume()
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
                                return executeRequest(item, queueType, isRetry);
                            });
                });
    }

    private Mono<Void> requeue(QueueItem item, QueueType queueType, boolean wasRetry) {
        if (wasRetry) {
            return globalQueueService.offerToRetry(item, queueType).then();
        }
        return globalQueueService.offer(item, queueType).then();
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
            log.error("Retry failed, dropping request for userId={}", item.getUserId());
            return Mono.empty();
        }

        // 재시도 가능한 에러인지 확인
        if (isRetryable(e) && item.canRetry()) {
            item.incrementRetryCount();
            log.warn("Retryable error, moving to retry queue userId={}", item.getUserId());
            return globalQueueService.offerToRetry(item, queueType).then();
        }

        log.error("Non-retryable error, dropping userId={}", item.getUserId());
        return Mono.empty();
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
