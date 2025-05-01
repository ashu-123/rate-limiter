package com.rateLimiter;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

class FixedWindowRateLimiterTest {

    private Jedis jedis;
    private FixedWindowRateLimiter fixedWindowRateLimiter;
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
    public void tearDown() {
        jedis.close();
    }

    @Test
    public void givenFixedWindowRateLimiterWhenRequestsInLimitThenShouldAllowAll() {
        fixedWindowRateLimiter = new FixedWindowRateLimiter(jedis, 10, 5);
        for (int i=1;i<=5;i++) {
            assertThat(fixedWindowRateLimiter.isAllowed("client1"))
                    .withFailMessage("Request " + i + " should be allowed")
                    .isTrue();
        }
    }

    @Test
    public void givenFixedWindowRateLimiterWhenRequestsLimitIsExceededThenShouldDeny() {
        fixedWindowRateLimiter = new FixedWindowRateLimiter(jedis, 60, 5);
        for (int i = 1; i <= 5; i++) {
            assertThat(fixedWindowRateLimiter.isAllowed("client1"))
                    .withFailMessage("Request " + i + " should be allowed")
                    .isTrue();
        }

        assertThat(fixedWindowRateLimiter.isAllowed("client1"))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();
    }

    @Test
    public void givenFixedWindowRateLimiterWhenWindowResetsThenShouldAllowRequestsAgain() throws InterruptedException {
        String clientId = "client1";
        fixedWindowRateLimiter = new FixedWindowRateLimiter(jedis, 1, 5);

        for (int i = 1; i <= 5; i++) {
            assertThat(fixedWindowRateLimiter.isAllowed(clientId))
                    .withFailMessage("Request " + i + " should be allowed")
                    .isTrue();
        }

        assertThat(fixedWindowRateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();

        Thread.sleep(2 * 1000);

        assertThat(fixedWindowRateLimiter.isAllowed(clientId))
                .withFailMessage("Request after window reset should be allowed")
                .isTrue();
    }

    @Test
    public void givenFixedWindowRateLimiterWhenRequestsFromMultipleClientsThenShouldHandleEachClientIndependently() {
        int limit = 5;
        String clientId1 = "client1";
        String clientId2 = "client2";
        int windowSize = 10;
        fixedWindowRateLimiter = new FixedWindowRateLimiter(jedis, windowSize, limit);

        for (int i = 1; i <= limit; i++) {
            assertThat(fixedWindowRateLimiter.isAllowed(clientId1))
                    .withFailMessage("Client 1 request " + i + " should be allowed")
                    .isTrue();
        }

        assertThat(fixedWindowRateLimiter.isAllowed(clientId1))
                .withFailMessage("Client 1 request beyond limit should be denied")
                .isFalse();

        for (int i = 1; i <= limit; i++) {
            assertThat(fixedWindowRateLimiter.isAllowed(clientId2))
                    .withFailMessage("Client 2 request " + i + " should be allowed")
                    .isTrue();
        }
    }

    @Test
    public void givenFixedWindowRateLimiterWhenAdditionalRequestsThenShouldDenyRequestsUntilWindowResets() throws InterruptedException {
        String clientId = "client1";
        fixedWindowRateLimiter = new FixedWindowRateLimiter(jedis, 5, 3);

        for (int i = 1; i <= 3; i++) {
            assertThat(fixedWindowRateLimiter.isAllowed(clientId))
                    .withFailMessage("Request " + i + " should be allowed within limit")
                    .isTrue();
        }

        assertThat(fixedWindowRateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();

        Thread.sleep(2500);

        assertThat(fixedWindowRateLimiter.isAllowed(clientId))
                .withFailMessage("Request should still be denied within the same fixed window")
                .isFalse();

        Thread.sleep(2500);

        assertThat(fixedWindowRateLimiter.isAllowed(clientId))
                .withFailMessage("Request should be allowed after fixed window reset")
                .isTrue();
    }

    @Test
    public void givenRedisCounterWhenRequestsDeniedThenDeniedRequestsShouldNotBeCounted() {
        String clientId = "client1";
        fixedWindowRateLimiter = new FixedWindowRateLimiter(jedis, 3, 3);

        for (int i = 1; i <= 3; i++) {
            assertThat(fixedWindowRateLimiter.isAllowed(clientId))
                    .withFailMessage("Request " + i + " should be allowed")
                    .isTrue();
        }

        assertThat(fixedWindowRateLimiter.isAllowed(clientId))
                .withFailMessage("This request should be denied")
                .isFalse();

        String key = "rate-limit: " + clientId;
        int requestCount = Integer.parseInt(jedis.get(key));
        assertThat(requestCount)
                .withFailMessage("The count (" + requestCount + ") should be equal to the limit (" + 3 + "), not counting the denied request")
                .isEqualTo(3);
    }

}