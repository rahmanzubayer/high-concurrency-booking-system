package com.zubayer.gatewayservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.web.server.SecurityWebFilterChain;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;

@Configuration
@EnableWebFluxSecurity
public class GatewaySecurityConfig {

    /**
     * Authentication is deliberately out of scope for this project.
     * <p>
     * In production the gateway validates a signed JWT here and extracts the subject
     * claim, which userKeyResolver then uses as the rate-limit key. Swapping it in is
     * a contained change: add spring-boot-starter-oauth2-resource-server, set
     * spring.security.oauth2.resourceserver.jwt.issuer-uri, and replace permitAll()
     * with authenticated() + .oauth2ResourceServer(oauth2 -> oauth2.jwt(...)).
     * <p>
     * This project is about concurrency control under load, so the X-User-Id header
     * stands in for a verified JWT claim.
     */
    @Bean
    public SecurityWebFilterChain gatewaySecurityFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .authorizeExchange(exchanges -> exchanges.anyExchange().permitAll())
                .build();
    }
}
