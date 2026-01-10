package com.jumunhasyeotjo.gateway.client;

import io.netty.handler.logging.LogLevel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.DefaultUriBuilderFactory;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.transport.logging.AdvancedByteBufFormat;


@Configuration
@Slf4j
public class WebClientConfig {
    @Value("${prometheus.url:prometheus:9090}")
    private String prometheusUrl;

    @Bean
    @LoadBalanced
    public WebClient.Builder webClientBuilder() {
        return WebClient.builder();
    }

    @Bean
    public WebClient webClient(WebClient.Builder builder) {

        HttpClient httpClient = HttpClient.create()
                .wiretap(
                        "reactor.netty.http.client.HttpClient",
                        LogLevel.INFO,
                        AdvancedByteBufFormat.TEXTUAL
                );

        return builder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .filter(logRequest())
                .filter(logResponse())
                .build();
    }

    @Bean("prometheusWebClient")
    public WebClient prometheusWebClient() {
        DefaultUriBuilderFactory factory = new DefaultUriBuilderFactory("http://"+prometheusUrl);
        factory.setEncodingMode(DefaultUriBuilderFactory.EncodingMode.VALUES_ONLY);

        return WebClient.builder()
                .uriBuilderFactory(factory)
                .build();
    }

    private ExchangeFilterFunction logRequest() {
        return ExchangeFilterFunction.ofRequestProcessor(request -> {
            log.info("➡ REQUEST {} {}", request.method(), request.url());
            request.headers().forEach((k, v) ->
                    v.forEach(value -> log.info("➡ HEADER {}={}", k, value))
            );
            return Mono.just(request);
        });
    }

    private ExchangeFilterFunction logResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(response -> {
            log.info("⬅ RESPONSE STATUS {}", response.statusCode());
            return Mono.just(response);
        });
    }
}
