package com.jumunhasyeotjo.gateway.ratelimiter.pg;

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
import reactor.core.publisher.Mono;

import java.net.URI;

@Slf4j
@Component
@RequiredArgsConstructor
public class TokenRefillListener {

    private final ReactiveRedisQueueService queueService;
    private final ReactiveRateLimiterService rateLimiterService;
    private final WebClient webClient;

    @Scheduled(fixedDelay = 100)
    public void processQueue() {
        // 큐에 아이템 있을 때만 토큰 소비
        queueService.getQueueSize()
                .filter(size -> size > 0)
                .flatMap(size -> rateLimiterService.findAvailableProvider())
                .flatMap(provider -> queueService.poll(1)
                        .flatMap(items -> {
                            if (items.isEmpty()) {
                                return Mono.empty();
                            }
                            return executeQueuedRequest(items.get(0), provider);
                        }))
                .subscribe();
    }

    private Mono<Void> executeQueuedRequest(QueueItem item, String provider) {
        if (item == null || item.getHttpRequest() == null) {
            return Mono.empty();
        }

        HttpRequestData request = item.getHttpRequest();
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
                .doOnSuccess(r -> log.info("Processed with provider {} for userId={}", provider, item.getUserId()))
                .then()
                .onErrorResume(e -> {
                    log.error("Request failed for userId={}", item.getUserId(), e);
                    return Mono.empty();
                });
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
