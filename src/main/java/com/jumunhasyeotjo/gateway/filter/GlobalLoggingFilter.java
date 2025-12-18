package com.jumunhasyeotjo.gateway.filter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import reactor.core.publisher.Mono;

@Slf4j
@Configuration
public class GlobalLoggingFilter {

    @Bean
    public GlobalFilter customGlobalFilter() {
        return (exchange, chain) -> {
            long startTime = System.currentTimeMillis();
            ServerHttpRequest request = exchange.getRequest();
            String path = request.getURI().getPath();
            String method = request.getMethod().name();

            // [REQUEST] Gateway 진입
            log.info(">>> [Gateway Request] Method=[{}] Path=[{}]", method, path);

            return chain.filter(exchange).then(Mono.fromRunnable(() -> {
                long duration = System.currentTimeMillis() - startTime;
                ServerHttpResponse response = exchange.getResponse();
                int statusCode = response.getStatusCode() != null ? response.getStatusCode().value() : 0;

                // [RESPONSE] Gateway 응답
                // 성공/실패 여부와 관계없이 Latency와 Status를 기록
                log.info("<<< [Gateway Response] Method=[{}] Path=[{}] Status=[{}] Duration=[{}ms]", 
                         method, path, statusCode, duration);
            }));
        };
    }
}