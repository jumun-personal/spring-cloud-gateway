package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.jumunhasyeotjo.gateway.ratelimiter.HttpRequestData;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.ReactiveRateLimiterService;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.ReactiveRedisQueueService;
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
    private final ReactiveRedisQueueService pgQueueService;
    private final WebClient webClient;

    @Scheduled(fixedDelay = 100)
    public void processQueue() {
        globalRateLimiterService.getAvailableTokens()
                .filter(tokens -> tokens > 0)
                .flatMap(tokens -> {
                    int batchSize = Math.min(tokens.intValue(), 10);
                    return globalQueueService.poll(batchSize);
                })
                .flatMapMany(Flux::fromIterable)
                .flatMap(item ->
                    globalRateLimiterService.tryConsume()
                            .flatMap(consumed -> {
                                if (consumed) {
                                    return processWithPgRateLimit(item);
                                }
                                log.debug("Global token exhausted, re-queuing userId={}", item.getUserId());
                                return globalQueueService.offer(item).then();
                            })
                , 1)
                .subscribe(
                        null,
                        e -> log.error("Queue processing error", e)
                );
    }

    private Mono<Void> processWithPgRateLimit(QueueItem item) {
        String provider = "TOSS"; // TODO: 요청에서 provider 추출 또는 라운드로빈

        return pgRateLimiterService.tryConsume(provider)
                .flatMap(hasToken -> {
                    if (hasToken) {
                        log.info("PG token consumed, executing request for userId={}", item.getUserId());
                        return executeRequest(item, provider);
                    }
                    // PG 토큰 없으면 PG 큐로
                    log.info("PG token exhausted, adding to PG queue userId={}", item.getUserId());
                    return pgQueueService.offer(item).then();
                });
    }

    private Mono<Void> executeRequest(QueueItem item, String provider) {
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
                        .queryParam("provider", provider)
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
                .doOnSuccess(r -> log.info("Processed request with provider {} for userId={}", provider, item.getUserId()))
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
