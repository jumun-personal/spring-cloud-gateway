package com.jumunhasyeotjo.gateway.ratelimiter.global;

import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Scale-Out 이벤트 발생 시 피드백 루프 상태를 관리하는 서비스
 *
 * - Scale-Out 이벤트 수신 시 현재 limit을 "이전 limit"(floor)으로 저장
 * - 피드백 루프 활성화/비활성화 관리
 * - Redis에 상태 영속화 (HA 지원)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FeedbackLoopStateManager {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final GlobalRateLimiterService rateLimiterService;

    private static final String STATE_KEY = "feedback:loop:state";
    private static final String FIELD_ACTIVE = "active";
    private static final String FIELD_PREVIOUS_LIMIT = "previousLimit";
    private static final String FIELD_TARGET_LIMIT = "targetLimit";
    private static final String FIELD_ACTIVATED_AT = "activatedAt";

    // In-memory cache for fast access
    private final AtomicBoolean active = new AtomicBoolean(false);
    private final AtomicInteger previousLimit = new AtomicInteger(0);
    private final AtomicInteger targetLimit = new AtomicInteger(0);
    private final AtomicLong activatedAt = new AtomicLong(0);

    /**
     * Scale-Out 이벤트 발생 시 피드백 루프 활성화
     * 현재 limit을 floor(previousLimit)로 저장
     */
    public Mono<FeedbackLoopState> activateOnScaleOut() {
        if (active.get()) {
            log.warn("Feedback loop already active since {}",
                    java.time.Instant.ofEpochMilli(activatedAt.get()));
            return Mono.just(getCurrentState());
        }

        int currentLimit = rateLimiterService.getCurrentLimit();
        long now = System.currentTimeMillis();

        FeedbackLoopState state = new FeedbackLoopState(true, currentLimit, 0, now);

        return persistState(state)
                .doOnSuccess(s -> {
                    active.set(true);
                    previousLimit.set(currentLimit);
                    targetLimit.set(0);
                    activatedAt.set(now);
                    log.info("Feedback loop activated. Previous limit (floor): {}", currentLimit);
                });
    }

    /**
     * 목표 limit 설정 (점진적 증가 목표)
     */
    public void setTargetLimit(int target) {
        targetLimit.set(target);
        persistField(FIELD_TARGET_LIMIT, String.valueOf(target)).subscribe();
        log.debug("Target limit set to: {}", target);
    }

    /**
     * 목표 limit 조회
     */
    public int getTargetLimit() {
        return targetLimit.get();
    }

    /**
     * floor limit (이전 limit) 조회
     */
    public int getPreviousLimit() {
        return previousLimit.get();
    }

    /**
     * 피드백 루프 활성 상태 확인
     */
    public boolean isActive() {
        return active.get();
    }

    /**
     * 피드백 루프 활성화 시간 조회
     */
    public long getActivatedAt() {
        return activatedAt.get();
    }

    /**
     * 피드백 루프 비활성화
     */
    public Mono<Void> deactivate() {
        return reactiveRedisTemplate.delete(STATE_KEY)
                .doOnSuccess(deleted -> {
                    active.set(false);
                    previousLimit.set(0);
                    targetLimit.set(0);
                    activatedAt.set(0);
                    log.info("Feedback loop deactivated");
                })
                .then();
    }

    /**
     * 현재 상태 조회
     */
    public FeedbackLoopState getCurrentState() {
        return new FeedbackLoopState(
                active.get(),
                previousLimit.get(),
                targetLimit.get(),
                activatedAt.get()
        );
    }

    /**
     * 서버 시작 시 Redis에서 상태 복구
     */
    @PostConstruct
    public void restoreState() {
        reactiveRedisTemplate.opsForHash().entries(STATE_KEY)
                .collectMap(
                        entry -> entry.getKey().toString(),
                        entry -> entry.getValue().toString()
                )
                .subscribe(
                        map -> {
                            if (map.isEmpty()) {
                                log.debug("No existing feedback loop state in Redis");
                                return;
                            }

                            boolean isActive = "true".equals(map.get(FIELD_ACTIVE));
                            int prevLimit = parseIntOrDefault(map.get(FIELD_PREVIOUS_LIMIT), 0);
                            int tgtLimit = parseIntOrDefault(map.get(FIELD_TARGET_LIMIT), 0);
                            long actAt = parseLongOrDefault(map.get(FIELD_ACTIVATED_AT), 0);

                            active.set(isActive);
                            previousLimit.set(prevLimit);
                            targetLimit.set(tgtLimit);
                            activatedAt.set(actAt);

                            if (isActive) {
                                log.info("Restored feedback loop state from Redis. " +
                                                "Active: {}, PreviousLimit: {}, TargetLimit: {}",
                                        isActive, prevLimit, tgtLimit);
                            }
                        },
                        error -> log.error("Failed to restore feedback loop state", error)
                );
    }

    private Mono<FeedbackLoopState> persistState(FeedbackLoopState state) {
        return reactiveRedisTemplate.opsForHash()
                .putAll(STATE_KEY, java.util.Map.of(
                        FIELD_ACTIVE, String.valueOf(state.isActive()),
                        FIELD_PREVIOUS_LIMIT, String.valueOf(state.getPreviousLimit()),
                        FIELD_TARGET_LIMIT, String.valueOf(state.getTargetLimit()),
                        FIELD_ACTIVATED_AT, String.valueOf(state.getActivatedAt())
                ))
                .thenReturn(state);
    }

    private Mono<Boolean> persistField(String field, String value) {
        return reactiveRedisTemplate.opsForHash()
                .put(STATE_KEY, field, value);
    }

    private int parseIntOrDefault(String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private long parseLongOrDefault(String value, long defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FeedbackLoopState {
        private boolean active;
        private int previousLimit;
        private int targetLimit;
        private long activatedAt;
    }
}