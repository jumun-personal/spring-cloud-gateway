package com.jumunhasyeotjo.gateway;

import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayConfig {

    @Bean
    public RouteLocator routes(RouteLocatorBuilder builder) {
        return builder.routes()
            .route("user-interact", r -> r
                .path("/v1/auth/**", "/v1/users/**", "/v1/messages/**")
                .uri("http://localhost:8085")
            )
            .route("order-shipping-service", r -> r
                .path("/v1/orders/**", "/v1/shippings/**")
                .uri("http://localhost:8086")
            )
            .route("hub-product-stock-company", r -> r
                .path("/api/v1/hubs/**", "/api/v1/stocks/**", "/api/v1/products/**", "/api/v1/companies/**")
                .uri("http://localhost:8087")
            )
            .build();
    }
}
