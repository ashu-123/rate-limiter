package com.rateLimiter;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.*;

class SlidingWindowRateLimiterTest {

    private Jedis jedis;
    private SlidingWindowRateLimiter slidingWindowRateLimiter;
    private static final RedisContainer redisContainer = new RedisContainer("redis:latest").withExposedPorts(6379);

    static {
        redisContainer.start();
    }

    @BeforeEach
    public void setup() {
        jedis = new Jedis(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        jedis.flushAll();
    }

    @AfterEach
    public void tearDown() { jedis.close(); }

}