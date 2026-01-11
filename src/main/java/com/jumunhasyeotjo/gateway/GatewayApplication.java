package com.jumunhasyeotjo.gateway;

import com.jumunhasyeotjo.gateway.ratelimiter.global.FeedbackLoopProperties;
import jakarta.annotation.PostConstruct;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import reactor.core.publisher.Hooks;

@SpringBootApplication(exclude = { SecurityAutoConfiguration.class })
@EnableScheduling
@EnableConfigurationProperties(FeedbackLoopProperties.class)
public class GatewayApplication {

	public static void main(String[] args) {
		SpringApplication.run(GatewayApplication.class, args);
	}

	@PostConstruct
	public void init() {
		Hooks.enableAutomaticContextPropagation();
	}
}
