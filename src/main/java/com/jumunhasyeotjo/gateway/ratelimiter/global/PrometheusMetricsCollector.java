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

    private final WebClient prometheusWebClient;

    public Mono<MetricsSnapshot> collectOrderServiceMetrics() {
        return Mono.zip(
                        getP95Latency(),
                        getP99Latency(),
                        getConnectionPoolUsage()
                ).doOnNext(tuple -> log.info("metric data found: {}", tuple.toString()))
                .map(tuple -> {
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
        log.info("→ Querying Prometheus: {}", query.trim());

        // 쿼리를 미리 인코딩
        String encodedQuery = UriUtils.encodeQueryParam(query, StandardCharsets.UTF_8);
        String fullUrl = "/api/v1/query?query=" + encodedQuery;

        return prometheusWebClient.get()
                .uri(fullUrl)  // 인코딩된 전체 URL 사용
                .retrieve()
                .bodyToMono(PrometheusResponse.class)
                .doOnNext(response -> {
                    log.info("← Prometheus Response: status={}, resultType={}, resultCount={}",
                            response.getStatus(),
                            response.getData() != null ? response.getData().getResultType() : "null",
                            response.getData() != null && response.getData().getResult() != null
                                    ? response.getData().getResult().size()
                                    : 0
                    );

                    if (response.getData() != null && response.getData().getResult() != null) {
                        log.info("← Full Response Data: {}", response.getData().getResult());
                    }
                })
                .map(response -> {
                    if (response.getData() != null
                            && response.getData().getResult() != null
                            && !response.getData().getResult().isEmpty()) {

                        String value = response.getData().getResult().get(0).getValue()[1].toString();
                        log.info("Extracted value: {}", value);
                        return Double.parseDouble(value);
                    }
                    log.warn("Empty response for query: {}", query.trim());
                    return 0.0;
                })
                .doOnError(e -> log.error("Prometheus query failed for: {}, error: {}",
                        query.trim(), e.getMessage(), e))
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
