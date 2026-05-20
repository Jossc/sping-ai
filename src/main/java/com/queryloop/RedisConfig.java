package com.queryloop;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

/**
 * Redis 连接池 & 序列化配置 — Phase 3 分布式改造
 *
 * 对应 application.yml:
 *   queryloop.redis.host / port / max-connections / timeout
 */
@Configuration
@ConfigurationProperties(prefix = "queryloop.redis")
@Data
public class RedisConfig {

    private String host = "localhost";
    private int port = 6379;
    private int maxConnections = 20;
    private int minIdleConnections = 5;
    private long timeoutMs = 2000;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(host, port);

        JedisClientConfiguration clientConfig = JedisClientConfiguration.builder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .readTimeout(Duration.ofMillis(timeoutMs))
                .usePooling()
                .poolConfig(poolConfig())
                .build();

        return new JedisConnectionFactory(config, clientConfig);
    }

    private redis.clients.jedis.JedisPoolConfig poolConfig() {
        redis.clients.jedis.JedisPoolConfig pool = new redis.clients.jedis.JedisPoolConfig();
        pool.setMaxTotal(maxConnections);
        pool.setMinIdle(minIdleConnections);
        pool.setMaxIdle(maxConnections);
        pool.setMaxWait(Duration.ofMillis(timeoutMs));
        pool.setTestOnBorrow(true);
        return pool;
    }
}
