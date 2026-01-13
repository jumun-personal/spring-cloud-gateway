package com.jumunhasyeotjo.gateway.filter;

import com.jumunhasyeotjo.gateway.ratelimiter.global.LatencyHistogramService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Gateway → Order Service RTT 측정 필터
 * Redis Time-Sliced Histogram에 기록
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LatencyMeasurementFilter implements GlobalFilter, Ordered {

    private final LatencyHistogramService latencyHistogramService;

    private static final String START_TIME_ATTR = "gatewayStartTime";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();

        // Order Service 요청만 측정
        if (!path.startsWith("/api/v1/orders")) {
            return chain.filter(exchange);
        }

        // 시작 시간 기록
        exchange.getAttributes().put(START_TIME_ATTR, System.currentTimeMillis());

        return chain.filter(exchange)
                .doFinally(signalType -> {
                    Long startTime = exchange.getAttribute(START_TIME_ATTR);
                    if (startTime != null) {
                        long latencyMs = System.currentTimeMillis() - startTime;
                        
                        // Histogram에 기록 (비동기)
                        latencyHistogramService.recordLatency(latencyMs)
                                .subscribe(
                                        null,
                                        e -> log.debug("Failed to record latency", e)
                                );

                        log.debug("Request latency: {}ms, path: {}", latencyMs, path);
                    }
                });
    }

    @Override
    public int getOrder() {
        // GlobalRateLimitFilter (-2) 보다 먼저 실행
        return -3;
    }
}
