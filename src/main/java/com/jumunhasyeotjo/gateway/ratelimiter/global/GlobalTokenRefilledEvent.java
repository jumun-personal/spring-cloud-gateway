package com.jumunhasyeotjo.gateway.ratelimiter.global;


import org.springframework.context.ApplicationEvent;

public class GlobalTokenRefilledEvent extends ApplicationEvent {

    public GlobalTokenRefilledEvent(Object source) {
        super(source);
    }
}
