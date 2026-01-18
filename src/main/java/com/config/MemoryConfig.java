package com.config;

import com.service.RedisChatMemory;
import org.springframework.ai.chat.memory.ChatMemory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class MemoryConfig {

    @Bean
    public ChatMemory chatMemory(StringRedisTemplate redisTemplate) {
        return new RedisChatMemory(redisTemplate);
    }
}
