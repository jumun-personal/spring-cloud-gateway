package com.jumunhasyeotjo.gateway.ratelimiter.global.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Scale 이벤트 응답 DTO
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ScaleEventResponse {
    private boolean accepted;        // 요청 수락 여부
    private int previousLimit;       // floor limit (이전 limit)
    private int currentLimit;        // 현재 limit
    private String message;          // 응답 메시지
}