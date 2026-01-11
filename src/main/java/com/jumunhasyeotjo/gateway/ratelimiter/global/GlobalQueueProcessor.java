package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.jumunhasyeotjo.gateway.ratelimiter.HttpRequestData;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
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

    private final GlobalQueueService queueService;
    private final GlobalRateLimiterService rateLimiterService;
    private final WebClient webClient;

    @Scheduled(fixedDelay = 100)
    public void processQueue() {
        // 1. 가용 토큰 확인
        rateLimiterService.getAvailableTokens()
                .filter(tokens -> tokens > 0)
                .flatMap(tokens -> {
                    // 2. 토큰 수만큼 큐에서 꺼내기
                    int batchSize = Math.min(tokens.intValue(), 10);
                    return queueService.poll(batchSize);
                })
                .flatMapMany(Flux::fromIterable)
                .flatMap(item -> 
                    // 3. 토큰 소비 시도 후 요청 실행
                    rateLimiterService.tryConsume()
                            .flatMap(consumed -> {
                                if (consumed) {
                                    return executeRequest(item);
                                }
                                // 토큰 소진 시 다시 큐에
                                log.debug("Token exhausted, re-queuing userId={}", item.getUserId());
                                return queueService.offer(item).then();
                            })
                , 1) // concurrency 1로 순차 처리
                .subscribe(
                        null,
                        e -> log.error("Queue processing error", e)
                );
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
                .doOnSuccess(r -> log.info("Processed queued request for userId={}", item.getUserId()))
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
