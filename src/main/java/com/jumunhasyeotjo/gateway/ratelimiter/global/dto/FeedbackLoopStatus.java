package com.jumunhasyeotjo.gateway.ratelimiter.global.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 피드백 루프 상태 응답 DTO
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FeedbackLoopStatus {
    private boolean active;              // 피드백 루프 활성화 여부
    private int previousLimit;           // floor limit (이전 limit)
    private int currentLimit;            // 현재 limit
    private int targetLimit;             // 목표 limit (증가 중일 때)
    private long activeSince;            // 활성화 시작 시간 (epoch ms)
    private Double p95Latency;           // 현재 P95 레이턴시 (ms)
    private Double p99Latency;           // 현재 P99 레이턴시 (ms)
}