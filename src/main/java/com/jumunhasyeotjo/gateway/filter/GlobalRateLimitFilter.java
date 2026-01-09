package com.jumunhasyeotjo.gateway.filter;


import com.jumunhasyeotjo.gateway.jwt.JwtProvider;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalQueueService;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalRateLimiterService;
import com.jumunhasyeotjo.gateway.ratelimiter.HttpRequestData;
import io.jsonwebtoken.Claims;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class GlobalRateLimitFilter implements GlobalFilter, Ordered {

    private final GlobalRateLimiterService rateLimiterService;
    private final GlobalQueueService queueService;
    private final JwtProvider jwtProvider;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();

        return rateLimiterService.tryConsume()
                .flatMap(hasToken -> {
                    if (hasToken) {
                        log.debug("Request passed rate limit: {}", path);
                        return chain.filter(exchange);
                    }

                    log.info("Request rate limited, adding to global queue: {}", path);
                    return addToGlobalQueue(exchange);
                });
    }

    private Mono<Void> addToGlobalQueue(ServerWebExchange exchange) {
        Long userId = extractUserId(exchange);
        String accessToken = extractAccessToken(exchange);

        return captureRequest(exchange)
                .flatMap(httpRequest -> {
                    QueueItem item = new QueueItem(userId, accessToken, httpRequest);
                    return queueService.offer(item);
                })
                .flatMap(added -> queueService.findSequence(userId))
                .flatMap(sequence -> {
                    ServerHttpResponse response = exchange.getResponse();
                    response.setStatusCode(HttpStatus.ACCEPTED);

                    // getHeaders()는 이미 mutable
                    response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

                    String body = String.format(
                            "{\"status\":\"queued\",\"position\":%d,\"currentLimit\":%d,\"algorithm\":\"sliding-window\"}",
                            sequence, rateLimiterService.getCurrentLimit()
                    );

                    DataBuffer buffer = response.bufferFactory()
                            .wrap(body.getBytes(StandardCharsets.UTF_8));

                    return response.writeWith(Mono.just(buffer));
                })
                .onErrorResume(e -> {
                    log.error("Error adding to global queue", e);
                    ServerHttpResponse response = exchange.getResponse();
                    response.setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
                    return response.setComplete();
                });
    }

    private Long extractUserId(ServerWebExchange exchange) {
        String token = exchange.getRequest().getHeaders().getFirst("Authorization");

        if (token == null || !token.startsWith("Bearer ")) {
            return null;
        }

        try {
            String jwt = token.substring(7);
            Claims claims = jwtProvider.getClaims(jwt);
            return claims.get("userId", Long.class);
        } catch (Exception e) {
            log.error("Failed to extract userId from JWT", e);
            return null;
        }
    }

    private String extractAccessToken(ServerWebExchange exchange) {
        String auth = exchange.getRequest().getHeaders().getFirst("Authorization");
        return auth != null ? auth.replace("Bearer ", "") : null;
    }

    private Mono<HttpRequestData> captureRequest(ServerWebExchange exchange) {
        ServerHttpRequest request = exchange.getRequest();

        Map<String, String> headers = new HashMap<>();
        request.getHeaders().forEach((key, values) ->
                headers.put(key, String.join(",", values))
        );

        return DataBufferUtils.join(request.getBody())
                .map(dataBuffer -> {
                    byte[] bytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bytes);
                    DataBufferUtils.release(dataBuffer);

                    return new String(bytes, StandardCharsets.UTF_8);
                })
                .defaultIfEmpty("")
                .map(body -> new HttpRequestData(
                        request.getMethod().name(),
                        request.getURI().toString(),
                        headers,
                        body
                ));
    }


    @Override
    public int getOrder() {
        return -2; // PG Rate Limit보다 먼저 실행
    }
}
