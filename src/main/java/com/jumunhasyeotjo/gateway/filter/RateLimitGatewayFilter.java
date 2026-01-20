package com.jumunhasyeotjo.gateway.filter;

import com.jumunhasyeotjo.gateway.jwt.JwtProvider;
import com.jumunhasyeotjo.gateway.ratelimiter.HttpRequestData;
import com.jumunhasyeotjo.gateway.ratelimiter.dto.RateLimitRequest;
import com.jumunhasyeotjo.gateway.ratelimiter.dto.RateLimitResponse;
import io.jsonwebtoken.Claims;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
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
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class RateLimitGatewayFilter implements GlobalFilter, Ordered {

    private final JwtProvider jwtProvider;
    private final WebClient rateLimiterClient;

    public RateLimitGatewayFilter(
            JwtProvider jwtProvider,
            @Value("${ratelimiter.service.url:http://localhost:8087}") String rateLimiterServiceUrl) {
        this.jwtProvider = jwtProvider;
        this.rateLimiterClient = WebClient.builder()
                .baseUrl(rateLimiterServiceUrl)
                .build();
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();

        // /api/v1/orders/bf만 체크
        if (!path.startsWith("/api/v1/orders/bf")) {
            return chain.filter(exchange);
        }

        // provider 파라미터 추출
        String provider = exchange.getRequest().getQueryParams().getFirst("provider");
        if (provider == null) {
            provider = "TOSS"; // 기본값
        }

        Long userId = extractUserId(exchange);
        String accessToken = extractAccessToken(exchange);
        String finalProvider = provider;

        return captureRequest(exchange)
                .flatMap(httpRequest -> {
                    RateLimitRequest request = RateLimitRequest.builder()
                            .path(path)
                            .userId(userId)
                            .accessToken(accessToken)
                            .httpRequest(httpRequest)
                            .provider(finalProvider)
                            .build();

                    return callRateLimiterService(request);
                })
                .flatMap(response -> {
                    if (response.isAllowed()) {
                        log.debug("Request passed PG rate limit: {} (provider={})", path, finalProvider);
                        return chain.filter(exchange);
                    }

                    if (response.isQueued()) {
                        log.info("Request queued for PG: path={}, provider={}, position={}",
                                path, finalProvider, response.getQueuePosition());
                        return respondQueued(exchange, response);
                    }

                    log.warn("Request rejected by PG rate limit: {} (provider={})", path, finalProvider);
                    return respondRejected(exchange);
                })
                .onErrorResume(e -> {
                    log.error("Error calling ratelimiter-service, allowing request: {}", e.getMessage());
                    return chain.filter(exchange);
                });
    }

    private Mono<RateLimitResponse> callRateLimiterService(RateLimitRequest request) {
        return rateLimiterClient.post()
                .uri("/api/v1/ratelimit/check")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(RateLimitResponse.class)
                .doOnError(e -> log.error("Failed to call ratelimiter-service: {}", e.getMessage()));
    }

    private Mono<Void> respondQueued(ServerWebExchange exchange, RateLimitResponse response) {
        ServerHttpResponse httpResponse = exchange.getResponse();
        httpResponse.setStatusCode(HttpStatus.ACCEPTED);
        httpResponse.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        String body = String.format(
                "{\"status\":\"queued\",\"queueType\":\"%s\",\"position\":%d,\"currentLimit\":%d}",
                response.getQueueType() != null ? response.getQueueType() : "PG",
                response.getQueuePosition() != null ? response.getQueuePosition() : 0,
                response.getCurrentLimit()
        );

        DataBuffer buffer = httpResponse.bufferFactory()
                .wrap(body.getBytes(StandardCharsets.UTF_8));

        return httpResponse.writeWith(Mono.just(buffer));
    }

    private Mono<Void> respondRejected(ServerWebExchange exchange) {
        ServerHttpResponse httpResponse = exchange.getResponse();
        httpResponse.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        return httpResponse.setComplete();
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
        return -1;
    }
}
