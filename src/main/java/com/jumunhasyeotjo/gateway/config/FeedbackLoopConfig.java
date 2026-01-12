package com.jumunhasyeotjo.gateway.config;

import com.jumunhasyeotjo.gateway.ratelimiter.global.FeedbackLoopProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(FeedbackLoopProperties.class)
public class FeedbackLoopConfig {

}
