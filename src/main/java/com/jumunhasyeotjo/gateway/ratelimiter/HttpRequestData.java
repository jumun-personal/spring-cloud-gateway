package com.jumunhasyeotjo.gateway.ratelimiter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HttpRequestData {
    private String method;
    private String uri;
    private Map<String, String> headers;
    private String body;
}