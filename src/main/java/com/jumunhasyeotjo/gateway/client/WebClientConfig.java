package com.jumunhasyeotjo.gateway.client;

import io.netty.channel.ChannelOption;
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

import java.time.Duration;


@Configuration
@Slf4j
public class WebClientConfig {

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
                )
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 20000)  // 연결 2초
                .responseTimeout(Duration.ofSeconds(3));             // 응답 3초

        return builder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
