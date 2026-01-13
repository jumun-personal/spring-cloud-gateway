package com.jumunhasyeotjo.gateway.filter;

import com.jumunhasyeotjo.gateway.jwt.JwtProvider;
import com.jumunhasyeotjo.gateway.ratelimiter.QueueItem;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalQueueService;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalQueueService.QueueType;
import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalRateLimiterService;
import io.jsonwebtoken.Claims;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

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

        // /api/v1/orders만 체크
        if (!path.startsWith("/api/v1/orders")) {
            return chain.filter(exchange);
        }

        // 신규 요청: 대기열 비어있고 토큰 있을 때만 통과
        return rateLimiterService.tryConsumeForNewRequest()
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
        String idempotencyKey = exchange.getRequest().getHeaders().getFirst("x-idempotency-key");
        String uri = exchange.getRequest().getURI().toString();
        HttpMethod method = exchange.getRequest().getMethod();
        QueueType queueType = queueService.resolveQueueType(method, uri);

        log.info("Adding to {} queue: userId={}", queueType, userId);

        // Command 기반 QueueItem 생성 (최소 정보만)
        QueueItem item;
        if (queueType == QueueType.ORDER) {
            item = QueueItem.createOrder(userId, UUID.randomUUID(), idempotencyKey);
        } else {
            item = QueueItem.createGeneric("OTHER_REQUEST", userId, null);
        }

        return queueService.offer(item, queueType)
                .flatMap(added -> queueService.findSequence(userId, queueType))
                .flatMap(sequence -> {
                    ServerHttpResponse response = exchange.getResponse();
                    response.setStatusCode(HttpStatus.ACCEPTED);
                    response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

                    String body = String.format(
                            "{\"status\":\"queued\",\"queueType\":\"%s\",\"position\":%d,\"requestId\":\"%s\"}",
                            queueType.name(), sequence, item.getRequestId()
                    );

                    DataBuffer buffer = response.bufferFactory()
                            .wrap(body.getBytes(StandardCharsets.UTF_8));

                    return response.writeWith(Mono.just(buffer));
                })
                .onErrorResume(e -> {
                    log.error("Error adding to global queue", e);
                    ServerHttpResponse response = exchange.getResponse();
                    
                    // 대기열 가득 참 에러 처리
                    if (e.getMessage() != null && e.getMessage().contains("대기열이 가득")) {
                        response.setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
                        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
                        String body = "{\"error\":\"QUEUE_FULL\",\"message\":\"대기열이 가득 찼습니다. 잠시 후 다시 시도해주세요.\"}";
                        DataBuffer buffer = response.bufferFactory().wrap(body.getBytes(StandardCharsets.UTF_8));
                        return response.writeWith(Mono.just(buffer));
                    }
                    
                    response.setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
                    return response.setComplete();
                });
    }

    private Long extractUserId(ServerWebExchange exchange) {
        String token = exchange.getRequest().getHeaders().getFirst("Authorization");

        if (token == null || !token.startsWith("Bearer ")) {
            return 0L;
        }

        try {
            String jwt = token.substring(7);
            Claims claims = jwtProvider.getClaims(jwt);
            return claims.get("userId", Long.class);
        } catch (Exception e) {
            log.error("Failed to extract userId from JWT", e);
            return 0L;
        }
    }

    @Override
    public int getOrder() {
        return -2;
    }
}
