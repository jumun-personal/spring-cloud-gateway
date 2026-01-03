package com.jumunhasyeotjo.gateway.filter;

import com.jumunhasyeotjo.gateway.client.dto.PassportRes;
import com.jumunhasyeotjo.gateway.jwt.JwtProvider;
import io.jsonwebtoken.Claims;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class JwtAuthenticationFilter implements WebFilter {

    private final JwtProvider jwtProvider;
    private final WebClient webClient;  // ← Builder 말고 WebClient로 변경!

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String BearerToken = exchange.getRequest().getHeaders().getFirst("Authorization");
        String token = jwtProvider.resolveToken(exchange.getRequest());

        if (token == null) {
            return chain.filter(exchange);
        }

        if (!jwtProvider.validateToken(token)) {
            exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
            return exchange.getResponse().setComplete();
        }

        Claims claims = jwtProvider.getClaims(token);
        String name = claims.getSubject();
        Long userId = claims.get("userId", Long.class);
        String role = claims.get("role", String.class);

        return webClient  // ← 여기! webClientBuilder.build() 대신 webClient
                .post()
                .uri(uriBuilder ->
                        uriBuilder
                                .scheme("lb")
                                .host("user-service")
                                .path("/api/v1/passports")
                                .queryParam("jwt", BearerToken)
                                .build()
                )
                .retrieve()
                .bodyToMono(PassportRes.class)
                .flatMap(response -> {
                    log.info("webClient 호출, URL={}, Thread={}",
                            exchange.getRequest().getURI(), Thread.currentThread().getName());

                    if (response.passport() == null) {
                        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
                        return exchange.getResponse().setComplete();
                    }

                    UsernamePasswordAuthenticationToken authentication =
                            new UsernamePasswordAuthenticationToken(
                                    name,
                                    null,
                                    List.of(new SimpleGrantedAuthority(role))
                            );

                    log.info("Passport : " + response.passport());
                    exchange.getRequest().getHeaders().add("X-Passport", response.passport());

                    return chain.filter(exchange)
                            .contextWrite(ReactiveSecurityContextHolder.withAuthentication(authentication));
                })
                .onErrorResume(e -> {
                    log.error("Passport 검증 실패", e);
                    exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
                    return exchange.getResponse().setComplete();
                });
    }
}