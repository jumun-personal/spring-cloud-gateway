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
            .route("user-service", r -> r
                .path("/user/api/v1/auth/**", "/user/api/v1/users/**", "/user/api/v1/messages/**", "/user/api/v1/passports/**")
                .uri("lb://user-service")
            )
            .route("user-service-actuator", r -> r
                .path("/monitoring/user-service/actuator/**")
                .filters(f -> f.rewritePath(
                    "/monitoring/user-service/(?<segment>.*)",
                    "/${segment}"
                ))
                .uri("lb://user-service")
            )
            .route("order-to-shipping-service", r -> r
                .path("/order/api/v1/orders/**", "/order/api/v1/coupons/**", "/order/api/v1/shippings/**", "/order/api/v1/payments/**", "/order/api/v1/shipping-histories/**")
                .uri("lb://order-to-shipping-service")
            )
            .route("hub-product-stock-company", r -> r
                .path("/hub/api/v1/hubs/**", "/hub/api/v1/stocks/**", "/hub/api/v1/products/**", "/hub/api/v1/companies/**")
                .uri("lb://hub-product-stock-company")
            )
            .build();
    }
}
