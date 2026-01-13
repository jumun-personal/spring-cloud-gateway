package com.jumunhasyeotjo.gateway.ratelimiter.global;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Feedback Loop 제어 API
 * Scale Out 이벤트 수신용
 */
@Slf4j
@RestController
@RequestMapping("/api/internal/feedback-loop")
@RequiredArgsConstructor
public class FeedbackLoopController {

    private final FeedbackLoopScheduler feedbackLoopScheduler;
    private final LeakyBucketProperties leakyBucketProperties;
    private final LatencyHistogramService latencyHistogramService;

    /**
     * Scale Out 이벤트 수신 - Feedback Loop 시작
     */
    @PostMapping("/scale-out")
    public ResponseEntity<Map<String, Object>> onScaleOut() {
        log.info("Scale Out event received, starting feedback loop");
        feedbackLoopScheduler.startFeedbackLoop();

        Map<String, Object> response = new HashMap<>();
        response.put("status", "STARTED");
        response.put("previousLimit", feedbackLoopScheduler.getPreviousLimit());
        response.put("targetLimit", feedbackLoopScheduler.getTargetLimit());
        response.put("currentLimit", leakyBucketProperties.getGlobal().getCapacity());

        return ResponseEntity.ok(response);
    }

    /**
     * Feedback Loop 중지
     */
    @PostMapping("/stop")
    public ResponseEntity<Map<String, Object>> stop() {
        log.info("Stopping feedback loop");
        feedbackLoopScheduler.stopFeedbackLoop();

        Map<String, Object> response = new HashMap<>();
        response.put("status", "STOPPED");
        response.put("currentLimit", leakyBucketProperties.getGlobal().getCapacity());

        return ResponseEntity.ok(response);
    }

    /**
     * 현재 상태 조회
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        LatencyHistogramService.PercentileResult percentiles = 
                latencyHistogramService.getPercentiles().block();

        Map<String, Object> response = new HashMap<>();
        response.put("active", feedbackLoopScheduler.isActive());
        response.put("previousLimit", feedbackLoopScheduler.getPreviousLimit());
        response.put("targetLimit", feedbackLoopScheduler.getTargetLimit());
        response.put("currentLimit", leakyBucketProperties.getGlobal().getCapacity());

        if (percentiles != null) {
            response.put("p95", percentiles.getP95());
            response.put("p99", percentiles.getP99());
            response.put("sampleCount", percentiles.getTotal());
        }

        return ResponseEntity.ok(response);
    }

    /**
     * Histogram 초기화
     */
    @PostMapping("/histogram/reset")
    public ResponseEntity<Map<String, Object>> resetHistogram() {
        latencyHistogramService.reset().subscribe();
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "RESET");
        
        return ResponseEntity.ok(response);
    }
}
