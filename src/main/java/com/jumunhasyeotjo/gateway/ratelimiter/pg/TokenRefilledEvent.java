package com.jumunhasyeotjo.gateway.ratelimiter.pg;


import org.springframework.context.ApplicationEvent;

public class TokenRefilledEvent extends ApplicationEvent {

    public TokenRefilledEvent(Object source) {
        super(source);
    }
}
