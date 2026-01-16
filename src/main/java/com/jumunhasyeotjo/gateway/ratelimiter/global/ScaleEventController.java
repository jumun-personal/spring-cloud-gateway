package com.jumunhasyeotjo.gateway.ratelimiter.global;

import com.jumunhasyeotjo.gateway.ratelimiter.global.dto.FeedbackLoopStatus;
import com.jumunhasyeotjo.gateway.ratelimiter.global.dto.ScaleEventResponse;
import com.jumunhasyeotjo.gateway.ratelimiter.global.dto.ScaleOutEventRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

/**
 * Scale-Out 이벤트 수신 및 피드백 루프 관리 API
 *
 * - POST /internal/scale/out: Scale-Out 이벤트 수신, 피드백 루프 활성화
 * - GET /internal/scale/status: 피드백 루프 상태 조회
 * - POST /internal/scale/deactivate: 피드백 루프 수동 비활성화
 */
@Slf4j
@RestController
@RequestMapping("/internal/scale")
@RequiredArgsConstructor
public class ScaleEventController {

    private final FeedbackLoopStateManager stateManager;
    private final GlobalRateLimiterService rateLimiterService;
    private final RedisLatencyHistogramService histogramService;

    /**
     * Scale-Out 이벤트 수신
     * 피드백 루프를 활성화하고 현재 limit을 floor로 저장
     */
    @PostMapping("/out")
    public Mono<ResponseEntity<ScaleEventResponse>> handleScaleOut(
            @RequestBody ScaleOutEventRequest request) {

        log.info("Received scale-out event from: {}, newInstanceCount: {}",
                request.getSource(), request.getNewInstanceCount());

        if (stateManager.isActive()) {
            var state = stateManager.getCurrentState();
            return Mono.just(ResponseEntity.ok(ScaleEventResponse.builder()
                    .accepted(false)
                    .previousLimit(state.getPreviousLimit())
                    .currentLimit(rateLimiterService.getCurrentLimit())
                    .message("Feedback loop already active since " +
                            java.time.Instant.ofEpochMilli(state.getActivatedAt()))
                    .build()));
        }

        return stateManager.activateOnScaleOut()
                .map(state -> ResponseEntity.ok(ScaleEventResponse.builder()
                        .accepted(true)
                        .previousLimit(state.getPreviousLimit())
                        .currentLimit(rateLimiterService.getCurrentLimit())
                        .message("Feedback loop activated. Floor set at " + state.getPreviousLimit())
                        .build()));
    }

    /**
     * 피드백 루프 상태 조회
     */
    @GetMapping("/status")
    public Mono<ResponseEntity<FeedbackLoopStatus>> getStatus() {
        var state = stateManager.getCurrentState();
        int currentLimit = rateLimiterService.getCurrentLimit();

        if (!state.isActive()) {
            return Mono.just(ResponseEntity.ok(FeedbackLoopStatus.builder()
                    .active(false)
                    .previousLimit(0)
                    .currentLimit(currentLimit)
                    .targetLimit(0)
                    .activeSince(0)
                    .build()));
        }

        // 활성화 상태일 때 P95/P99 레이턴시도 조회
        return Mono.zip(
                histogramService.getP95Latency(),
                histogramService.getP99Latency()
        ).map(tuple -> ResponseEntity.ok(FeedbackLoopStatus.builder()
                .active(true)
                .previousLimit(state.getPreviousLimit())
                .currentLimit(currentLimit)
                .targetLimit(state.getTargetLimit())
                .activeSince(state.getActivatedAt())
                .p95Latency(tuple.getT1())
                .p99Latency(tuple.getT2())
                .build()));
    }

    /**
     * 피드백 루프 수동 비활성화
     */
    @PostMapping("/deactivate")
    public Mono<ResponseEntity<ScaleEventResponse>> deactivate() {
        if (!stateManager.isActive()) {
            return Mono.just(ResponseEntity.ok(ScaleEventResponse.builder()
                    .accepted(false)
                    .currentLimit(rateLimiterService.getCurrentLimit())
                    .message("Feedback loop is not active")
                    .build()));
        }

        int finalLimit = rateLimiterService.getCurrentLimit();

        return stateManager.deactivate()
                .thenReturn(ResponseEntity.ok(ScaleEventResponse.builder()
                        .accepted(true)
                        .currentLimit(finalLimit)
                        .message("Feedback loop deactivated. Final limit: " + finalLimit)
                        .build()));
    }
}