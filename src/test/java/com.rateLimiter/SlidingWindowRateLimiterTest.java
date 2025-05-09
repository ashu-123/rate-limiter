package com.rateLimiter;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void givenSlidingWindowRateLimiterWhenRequestCountInLimitThenShouldAllowAll() {
        slidingWindowRateLimiter = new SlidingWindowRateLimiter(jedis, 10, 1, 5);
        for (int i = 1; i <= 5; i++) {
            assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
        }
    }

    @Test
    public void givenSlidingWindowRateLimiterWhenRequestCountLimitIsExceededThenShouldDeny() {
        slidingWindowRateLimiter = new SlidingWindowRateLimiter(jedis, 60, 1, 5);
        for (int i = 1; i <= 5; i++) {
            assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
        }

        assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();
    }

    @Test
    public void givenSlidingWindowRateLimiterWhenSlidingWindowResetsThenShouldAllowRequestsAgain() throws InterruptedException {
        slidingWindowRateLimiter = new SlidingWindowRateLimiter(jedis, 2, 1, 5);

        for (int i = 1; i <= 5; i++) {
            assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
        }

        assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();

        Thread.sleep((2 + 1) * 1000);

        assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                .withFailMessage("Request after window reset should be allowed")
                .isTrue();
    }

    @Test
    public void givenSlidingWindowRateLimiterWhenRequestsFromMultipleClientsThenShouldHandleEachClientIndependently() {
        slidingWindowRateLimiter = new SlidingWindowRateLimiter(jedis, 10, 1, 5);

        for (int i = 1; i <= 5; i++) {
            assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                    .withFailMessage("Client 1 request %d should be allowed", i)
                    .isTrue();
        }

        assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                .withFailMessage("Client 1 request beyond limit should be denied")
                .isFalse();

        for (int i = 1; i <= 5; i++) {
            assertThat(slidingWindowRateLimiter.isAllowed("client2"))
                    .withFailMessage("Client 2 request %d should be allowed", i)
                    .isTrue();
        }
    }

    @Test
    public void givenSlidingWindowRateLimiterWhenSubWindowExpiresThenShouldAllowRequestsAgainGradually() throws InterruptedException {
        slidingWindowRateLimiter = new SlidingWindowRateLimiter(jedis, 4, 1, 3);

        for (int i = 1; i <= 3; i++) {
            assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
            Thread.sleep(1000);
        }

        assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();

        Thread.sleep(2000);

        assertThat(slidingWindowRateLimiter.isAllowed("client1"))
                .withFailMessage("Request should be allowed in a sliding window")
                .isTrue();
    }

}