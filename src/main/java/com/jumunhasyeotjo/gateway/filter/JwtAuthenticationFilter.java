package com.jumunhasyeotjo.gateway.filter;

import com.jumunhasyeotjo.gateway.jwt.JwtProvider;
import io.jsonwebtoken.Claims;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@RequiredArgsConstructor
public class JwtAuthenticationFilter implements WebFilter {

    private final JwtProvider jwtProvider;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {

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

        UsernamePasswordAuthenticationToken authentication =
            new UsernamePasswordAuthenticationToken(
                name,
                null,
                List.of(new SimpleGrantedAuthority(role))
            );

        ServerHttpRequest mutatedRequest = exchange.getRequest()
            .mutate()
            .header("X-User-Id", String.valueOf(userId))
            .header("X-User-Name", name)
            .header("X-User-Role", role)
            .build();

        ServerWebExchange mutatedExchange = exchange.mutate()
            .request(mutatedRequest)
            .build();

        return chain.filter(mutatedExchange)
            .contextWrite(ReactiveSecurityContextHolder.withAuthentication(authentication));
    }
}
