package com.jumunhasyeotjo.gateway.ratelimiter.pg;

import com.jumunhasyeotjo.gateway.ratelimiter.HttpRequestData;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class TokenRefillListener implements ApplicationListener<TokenRefilledEvent> {

    private final ReactiveRedisQueueService queueService;
    private final ReactiveRateLimiterService rateLimiterService;
    private final WebClient webClient;

    @Override
    public void onApplicationEvent(TokenRefilledEvent event) {
        processQueueWithToken().subscribe();
    }

    private Mono<Void> processQueueWithToken() {
        // 토큰 있는 PG 찾기
        return rateLimiterService.findAvailableProvider()
                .flatMap(provider -> {
                    log.info("Found available provider: {}", provider);

                    // 대기열에서 1개 꺼내기
                    return queueService.poll(1)
                            .flatMap(items -> {
                                if (items.isEmpty()) {
                                    return Mono.empty();
                                }

                                QueueItem item = items.get(0);
                                return executeQueuedRequest(item, provider);
                            });
                })
                .switchIfEmpty(Mono.defer(() -> {
                    log.debug("No available provider with tokens");
                    return Mono.empty();
                }))
                .then();
    }

    private Mono<Void> executeQueuedRequest(QueueItem item, String provider) {
        if (item == null || item.getHttpRequest() == null) {
            log.warn("Invalid queue item, skipping");
            return Mono.empty();
        }

        HttpRequestData request = item.getHttpRequest();

        log.info("Processing queued request with provider {}: method={}, uri={}, userId={}",
                provider, request.getMethod(), request.getUri(), item.getUserId());

        // URI에서 path만 추출 (scheme, host 제거)
        String path = extractPath(request.getUri());

        return webClient
                .method(HttpMethod.valueOf(request.getMethod()))
                .uri(uriBuilder ->
                        uriBuilder
                                .scheme("lb")
                                .host("order-to-shipping-service")
                                .path(path)  // 전체 URI가 아닌 path만 사용
                                .queryParam("provider", provider)
                                .build()
                )
                .headers(headers -> {
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    if (request.getHeaders() != null) {
                        request.getHeaders().forEach(headers::add);
                    }
                    if (item.getAccessToken() != null) {
                        headers.set("Authorization", "Bearer " + item.getAccessToken());
                    }
                })
                .body(BodyInserters.fromValue(request.getBody()))
                .retrieve()
                .bodyToMono(String.class)
                .doOnSuccess(response ->
                        log.info("Queued request completed with provider {} for userId={}",
                                provider, item.getUserId())
                )
                .doOnError(e ->
                        log.error("Failed to process queued request with provider {} for userId={}: {}",
                                provider, item.getUserId(), e.getMessage())
                )
                .then()
                .onErrorResume(e -> {
                    log.error("Error executing queued request", e);
                    return Mono.empty();
                });
    }

    private String extractPath(String uri) {
        try {
            // URI에서 path만 추출
            java.net.URI parsed = new java.net.URI(uri);
            String path = parsed.getPath();
            String query = parsed.getQuery();

            if (query != null && !query.isEmpty()) {
                return path + "?" + query;
            }
            return path;
        } catch (Exception e) {
            log.warn("Failed to parse URI: {}, using as-is", uri);
            return uri;

        }
    }
}