package com.zubayer.bookingingestservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.data.redis.core.script.DefaultRedisScript;

@Configuration
public class RedisScriptConfig {

    @Bean
    public RedisScript<Long> decrementScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();

        script.setScriptSource(
                new ResourceScriptSource(
                        new ClassPathResource("scripts/decrement_stock.lua")
                )
        );
        script.setResultType(Long.class);

        return script;
    }

}
