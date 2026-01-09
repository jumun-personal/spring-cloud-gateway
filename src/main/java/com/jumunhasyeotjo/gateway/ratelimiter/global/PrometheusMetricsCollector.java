package com.jumunhasyeotjo.gateway.ratelimiter.global;


import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriUtils;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.charset.StandardCharsets;

@Slf4j
@Service
@RequiredArgsConstructor
public class PrometheusMetricsCollector {

    private final WebClient webClient;

    @Value("${prometheus.url:http://prometheus:9090}")
    private String prometheusUrl;

    public Mono<MetricsSnapshot> collectOrderServiceMetrics() {
        return Mono.zip(
                getP95Latency(),
                getP99Latency(),
                getConnectionPoolUsage()
        ).map(tuple -> {
            MetricsSnapshot snapshot = new MetricsSnapshot();
            snapshot.setP95Latency(tuple.getT1());
            snapshot.setP99Latency(tuple.getT2());
            snapshot.setConnectionPoolUsage(tuple.getT3());
            return snapshot;
        }).doOnError(e -> log.error("Failed to collect metrics from Prometheus", e));
    }

    private Mono<Double> getP95Latency() {
        String query = """
        histogram_quantile(0.95, sum(rate(http_server_requests_seconds_bucket{service="order"}[1m])) by (le))
        """;
        return queryPrometheus(query);
    }

    private Mono<Double> getP99Latency() {
        String query = """
        histogram_quantile(0.99, sum(rate(http_server_requests_seconds_bucket{service="order"}[1m])) by (le))
        """;
        return queryPrometheus(query);
    }

    private Mono<Double> getConnectionPoolUsage() {
        String query = """
        avg(hikaricp_connections_active{service="order"} / hikaricp_connections_max{service="order"}) * 100
        """;
        return queryPrometheus(query);
    }

    private Mono<Double> queryPrometheus(String query) {
        String encodedQuery = UriUtils.encodeQueryParam(query, StandardCharsets.UTF_8);

        URI uri = URI.create(
                prometheusUrl + "/api/v1/query?query=" + encodedQuery
        );

        return webClient.get()
                .uri(uri)
                .retrieve()
                .bodyToMono(PrometheusResponse.class)
                .map(response -> {
                    if (response.getData() != null
                            && response.getData().getResult() != null
                            && !response.getData().getResult().isEmpty()) {
                        return Double.parseDouble(
                                response.getData().getResult().get(0).getValue()[1].toString()
                        );
                    }
                    return 0.0;
                })
                .onErrorReturn(0.0);
    }

    @Data
    public static class MetricsSnapshot {
        private double p95Latency;
        private double p99Latency;
        private double connectionPoolUsage;
    }

    @Data
    private static class PrometheusResponse {
        private String status;
        private PrometheusData data;
    }

    @Data
    private static class PrometheusData {
        private String resultType;
        private java.util.List<PrometheusResult> result;
    }

    @Data
    private static class PrometheusResult {
        private java.util.Map<String, String> metric;
        private Object[] value;
    }
}
