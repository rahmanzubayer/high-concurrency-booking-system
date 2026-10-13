package com.zubayer.gatewayservice.config;

import io.lettuce.core.resource.Delay;
import io.lettuce.core.resource.ClientResources;
import org.springframework.context.annotation.Bean;
import io.lettuce.core.resource.DefaultClientResources;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class LettuceConfig {

    /**
     * Lettuce defaults to exponential reconnect backoff capped at 30 seconds. After a
     * multi-minute outage the client sits in a 30s sleep, so the service stays degraded
     * long after Redis is healthy — regardless of how often the circuit breaker probes.
     * <p>
     * Capped at 2s: recovery becomes bounded by the breaker's wait duration, which is
     * the knob we actually meant to control.
     */
    @Bean(destroyMethod = "shutdown")
    public ClientResources lettuceClientResources() {
        return DefaultClientResources.builder()
                .reconnectDelay(Delay.exponential(100, 2000, TimeUnit.MILLISECONDS, 2))
                .build();
    }

}
