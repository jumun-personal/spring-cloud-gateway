package com.jumunhasyeotjo.gateway.filter;

import com.jumunhasyeotjo.gateway.ratelimiter.global.FeedbackLoopStateManager;
import com.jumunhasyeotjo.gateway.ratelimiter.global.RedisLatencyHistogramService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Gateway → Order Service RTT 레이턴시 기록 필터
 *
 * - 피드백 루프 활성화 시에만 레이턴시 기록
 * - /api/v1/orders/** 경로만 대상
 * - Redis 히스토그램에 비동기로 기록
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LatencyRecordingFilter implements GlobalFilter, Ordered {

    private final RedisLatencyHistogramService histogramService;
    private final FeedbackLoopStateManager stateManager;

    private static final String ORDER_PATH_PREFIX = "/api/v1/orders";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // 피드백 루프가 비활성화 상태이면 기록하지 않음
        if (!stateManager.isActive()) {
            return chain.filter(exchange);
        }

        // Order 서비스 요청만 레이턴시 기록
        String path = exchange.getRequest().getURI().getPath();
        if (!path.startsWith(ORDER_PATH_PREFIX)) {
            return chain.filter(exchange);
        }

        long startTime = System.currentTimeMillis();

        return chain.filter(exchange)
                .then(Mono.fromRunnable(() -> {
                    long latency = System.currentTimeMillis() - startTime;

                    // 비동기로 히스토그램에 기록
                    histogramService.recordLatency(latency)
                            .subscribe(
                                    null,
                                    error -> log.warn("Failed to record latency: {}", error.getMessage()),
                                    () -> log.debug("Recorded latency: {}ms for path: {}", latency, path)
                            );
                }));
    }

    @Override
    public int getOrder() {
        // 다른 필터보다 먼저 실행하여 전체 RTT 측정
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }
}