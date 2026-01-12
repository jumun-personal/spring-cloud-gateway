package com.jumunhasyeotjo.gateway.ratelimiter.pg;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ReactiveRateLimiterService {

    private final Map<String, PaymentProviderRateLimiter> rateLimiterMap;

    // 생성자에서 List를 Map으로 변환
    public ReactiveRateLimiterService(List<PaymentProviderRateLimiter> rateLimiters) {
        this.rateLimiterMap = rateLimiters.stream()
                .collect(Collectors.toMap(
                        PaymentProviderRateLimiter::getProviderName,
                        Function.identity()
                ));
    }

    // 특정 PG 토큰 소비
    public Mono<Boolean> tryConsume(String provider) {
        PaymentProviderRateLimiter rateLimiter = rateLimiterMap.get(provider.toUpperCase());
        if (rateLimiter == null) {
            return Mono.error(new IllegalArgumentException("Unknown provider: " + provider));
        }
        return rateLimiter.tryConsume();
    }

    // 토큰 있는 아무 PG 찾기
    public Mono<String> findAvailableProvider() {
        return Flux.fromIterable(rateLimiterMap.values())
                .flatMap(rateLimiter ->
                        rateLimiter.tryConsume()
                                .flatMap(success ->
                                        success
                                                ? Mono.just(rateLimiter.getProviderName())
                                                : Mono.empty()
                                )
                )
                .filter(Objects::nonNull)
                .next() // 첫 번째로 토큰 있는 PG 반환
                .switchIfEmpty(Mono.empty()); // null 대신 empty 반환
    }

    public int getRateLimit(String provider) {
        PaymentProviderRateLimiter rateLimiter = rateLimiterMap.get(provider.toUpperCase());
        return rateLimiter != null ? rateLimiter.getRateLimit() : 0;
    }

    public Mono<Long> getAvailableTokens(String provider) {
        PaymentProviderRateLimiter rateLimiter = rateLimiterMap.get(provider.toUpperCase());
        return rateLimiter.getAvailableTokens();
    }
}