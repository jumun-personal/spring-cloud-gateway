package com.jumunhasyeotjo.gateway.jwt;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;

import java.security.Key;
import java.util.Base64;

@Slf4j
@Component
public class JwtProvider {

    @Value("${jwt.secret}")
    private String secret;

    private static final String BEARER_PREFIX = "Bearer ";

    private Key key;

    @PostConstruct
    public void init() {
        // Auth 서비스와 동일하게 Base64 decode
        byte[] bytes = Base64.getDecoder().decode(secret);
        this.key = Keys.hmacShaKeyFor(bytes);
    }

    /** Authorization 헤더에서 Bearer 토큰 추출 */
    public String resolveToken(ServerHttpRequest request) {
        String authHeader = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

        if (authHeader == null || !authHeader.startsWith(BEARER_PREFIX)) {
            return null;
        }

        return authHeader.substring(BEARER_PREFIX.length());
    }

    /** Bearer 제거 메서드 (Auth와 호환) */
    public String removePrefix(String token) {
        if (token != null && token.startsWith(BEARER_PREFIX)) {
            return token.substring(BEARER_PREFIX.length());
        }
        return token;
    }

    /** JWT 검증 */
    public boolean validateToken(String token) {
        try {
            Jwts.parserBuilder()
                .setSigningKey(key)
                .build()
                .parseClaimsJws(token);
            return true;

        } catch (SecurityException | MalformedJwtException e) {
            log.error("Invalid JWT signature.");
        } catch (ExpiredJwtException e) {
            log.error("Expired JWT token.");
        } catch (UnsupportedJwtException e) {
            log.error("Unsupported JWT token.");
        } catch (IllegalArgumentException e) {
            log.error("JWT claims is empty.");
        }

        return false;
    }

    /** Claims 추출 */
    public Claims getClaims(String token) {
        return Jwts.parserBuilder()
            .setSigningKey(key)
            .build()
            .parseClaimsJws(token)
            .getBody();
    }

    /** subject(name) 추출 */
    public String getSubject(String token) {
        return getClaims(token).getSubject();
    }

    /** role 추출 */
    public String getRole(String token) {
        return getClaims(token).get("role", String.class);
    }
}
