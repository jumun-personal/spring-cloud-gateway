package com.jumunhasyeotjo.gateway.ratelimiter.global.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Scale-Out 이벤트 요청 DTO
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ScaleOutEventRequest {
    private String source;           // 이벤트 소스 (예: kubernetes, aws-asg)
    private int newInstanceCount;    // Scale-Out 후 인스턴스 수
    private long timestamp;          // 이벤트 발생 시간
}