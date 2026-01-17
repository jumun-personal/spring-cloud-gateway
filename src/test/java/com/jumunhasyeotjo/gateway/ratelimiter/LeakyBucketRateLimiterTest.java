package com.jumunhasyeotjo.gateway.ratelimiter;

import com.jumunhasyeotjo.gateway.ratelimiter.global.GlobalRateLimiterService;
import com.jumunhasyeotjo.gateway.ratelimiter.pg.toss.TossPaymentRateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;

/**
 * Leaky Bucket Rate Limiter 단위 테스트
 *
 * 테스트 시나리오:
 * 1. 용량까지 요청 수락
 * 2. 버킷이 가득 찼을 때 거부
 * 3. leak 이후 요청 수락
 * 4. 동적 rate 조정
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LeakyBucketRateLimiterTest {

    @Mock
    private ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    private GlobalRateLimiterService globalRateLimiterService;
    private TossPaymentRateLimiter tossPaymentRateLimiter;

    @BeforeEach
    void setUp() {
        globalRateLimiterService = new GlobalRateLimiterService(reactiveRedisTemplate);
        globalRateLimiterService.init();  // @PostConstruct 수동 호출

        tossPaymentRateLimiter = new TossPaymentRateLimiter(reactiveRedisTemplate);
        tossPaymentRateLimiter.init();  // @PostConstruct 수동 호출
    }

    @SuppressWarnings("unchecked")
    private void mockRedisExecute(long returnValue) {
        // GlobalRateLimiterService: 5 KEYS + 5 ARGV
        lenient().doReturn(Flux.just(returnValue))
                .when(reactiveRedisTemplate)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString());
        // TossPaymentRateLimiter: 1 KEY + 4 ARGV (기존 형식 유지)
        lenient().doReturn(Flux.just(returnValue))
                .when(reactiveRedisTemplate)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString());
    }

    @SuppressWarnings("unchecked")
    private void mockRedisExecuteForWaterLevel(long returnValue) {
        lenient().doReturn(Flux.just(returnValue))
                .when(reactiveRedisTemplate)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString());
    }

    @SuppressWarnings("unchecked")
    private void mockRedisExecuteError() {
        // GlobalRateLimiterService: 5 KEYS + 5 ARGV
        lenient().doReturn(Flux.error(new RuntimeException("Redis connection failed")))
                .when(reactiveRedisTemplate)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString());
        // TossPaymentRateLimiter: 1 KEY + 4 ARGV
        lenient().doReturn(Flux.error(new RuntimeException("Redis connection failed")))
                .when(reactiveRedisTemplate)
                .execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("tryConsume - 요청이 수락되면 true 반환")
    void tryConsume_shouldReturnTrue_whenAccepted() {
        // Given: Lua script returns 1 (accepted)
        mockRedisExecute(1L);

        // When & Then
        StepVerifier.create(globalRateLimiterService.tryConsume())
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    @DisplayName("tryConsume - 버킷이 가득 찼을 때 false 반환")
    void tryConsume_shouldReturnFalse_whenBucketFull() {
        // Given: Lua script returns 0 (rejected - bucket full)
        mockRedisExecute(0L);

        // When & Then
        StepVerifier.create(globalRateLimiterService.tryConsume())
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    @DisplayName("tryConsume - Redis 오류 시 false 반환 (fail-safe)")
    void tryConsume_shouldReturnFalse_onError() {
        // Given: Redis error
        mockRedisExecuteError();

        // When & Then
        StepVerifier.create(globalRateLimiterService.tryConsume())
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    @DisplayName("getAvailableTokens - 남은 용량 반환")
    void getAvailableTokens_shouldReturnAvailableCapacity() {
        // Given: water level = 5 (represented as 5000 in Lua), capacity = 15
        // Available = 15 - 5 = 10
        mockRedisExecuteForWaterLevel(5000L);

        // When & Then
        StepVerifier.create(globalRateLimiterService.getAvailableTokens())
                .expectNext(10L)  // 15 - 5
                .verifyComplete();
    }

    @Test
    @DisplayName("getCurrentWindowCount - 현재 water level 반환")
    void getCurrentWindowCount_shouldReturnWaterLevel() {
        // Given: water level = 8
        mockRedisExecuteForWaterLevel(8000L);

        // When & Then
        StepVerifier.create(globalRateLimiterService.getCurrentWindowCount())
                .expectNext(8L)
                .verifyComplete();
    }

    @Test
    @DisplayName("increaseLimit - rate 증가")
    void increaseLimit_shouldIncreaseRate() {
        // Given: initial rate = 15
        int initialRate = globalRateLimiterService.getCurrentLimit();

        // When
        globalRateLimiterService.increaseLimit(5);

        // Then
        assertThat(globalRateLimiterService.getCurrentLimit())
                .isEqualTo(initialRate + 5);
    }

    @Test
    @DisplayName("increaseLimit - MAX_LIMIT(100) 초과 불가")
    void increaseLimit_shouldNotExceedMaxLimit() {
        // Given: increase to maximum
        globalRateLimiterService.increaseLimit(200);

        // Then: should be capped at 100
        assertThat(globalRateLimiterService.getCurrentLimit())
                .isEqualTo(100);
    }

    @Test
    @DisplayName("decreaseLimit - rate 감소")
    void decreaseLimit_shouldDecreaseRate() {
        // Given: initial rate = 15
        int initialRate = globalRateLimiterService.getCurrentLimit();

        // When
        globalRateLimiterService.decreaseLimit(3);

        // Then
        assertThat(globalRateLimiterService.getCurrentLimit())
                .isEqualTo(initialRate - 3);
    }

    @Test
    @DisplayName("decreaseLimit - MIN_LIMIT(10) 미만 불가")
    void decreaseLimit_shouldNotGoBelowMinLimit() {
        // When: try to decrease below minimum
        globalRateLimiterService.decreaseLimit(100);

        // Then: should be capped at 10
        assertThat(globalRateLimiterService.getCurrentLimit())
                .isEqualTo(10);
    }

    @Test
    @DisplayName("isTokenSaturated - 90% 이상일 때 true 반환")
    void isTokenSaturated_shouldReturnTrue_whenAbove90Percent() {
        // Given: water level = 14 out of 15 (93.3%)
        mockRedisExecuteForWaterLevel(14000L);

        // When & Then
        StepVerifier.create(globalRateLimiterService.isTokenSaturated())
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    @DisplayName("isTokenSaturated - 90% 미만일 때 false 반환")
    void isTokenSaturated_shouldReturnFalse_whenBelow90Percent() {
        // Given: water level = 10 out of 15 (66.7%)
        mockRedisExecuteForWaterLevel(10000L);

        // When & Then
        StepVerifier.create(globalRateLimiterService.isTokenSaturated())
                .expectNext(false)
                .verifyComplete();
    }

    // === TossPaymentRateLimiter Tests ===

    @Test
    @DisplayName("Toss - tryConsume 수락")
    void toss_tryConsume_shouldReturnTrue_whenAccepted() {
        // Given
        mockRedisExecute(1L);

        // When & Then
        StepVerifier.create(tossPaymentRateLimiter.tryConsume())
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    @DisplayName("Toss - tryConsume 거부")
    void toss_tryConsume_shouldReturnFalse_whenBucketFull() {
        // Given
        mockRedisExecute(0L);

        // When & Then
        StepVerifier.create(tossPaymentRateLimiter.tryConsume())
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    @DisplayName("Toss - getAvailableTokens")
    void toss_getAvailableTokens_shouldReturnAvailableCapacity() {
        // Given: water level = 3 out of 10
        mockRedisExecuteForWaterLevel(3000L);

        // When & Then
        StepVerifier.create(tossPaymentRateLimiter.getAvailableTokens())
                .expectNext(7L)  // 10 - 3
                .verifyComplete();
    }

    @Test
    @DisplayName("Toss - getProviderName")
    void toss_getProviderName_shouldReturnTOSS() {
        assertThat(tossPaymentRateLimiter.getProviderName())
                .isEqualTo("TOSS");
    }

    @Test
    @DisplayName("Toss - getRateLimit")
    void toss_getRateLimit_shouldReturn10() {
        assertThat(tossPaymentRateLimiter.getRateLimit())
                .isEqualTo(10);
    }
}
