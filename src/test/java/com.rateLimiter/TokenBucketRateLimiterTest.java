package com.rateLimiter;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class TokenBucketRateLimiterTest {

    private Jedis jedis;
    private TokenBucketRateLimiter tokenBucketRateLimiter;
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
    void givenTokenBucketRateLimiterWhenTokenCountWithinBucketCapacityThenShouldAllowRequests() {
        tokenBucketRateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);
        for (int i = 1; i <= 5; i++) {
            assertThat(tokenBucketRateLimiter.isAllowed("client1"))
                    .withFailMessage("Request %d should be allowed within bucket capacity", i)
                    .isTrue();
        }
    }

    @Test
    void givenTokenBucketRateLimiterWhenBucketIsEmptyThenShouldDenyRequests() {
        tokenBucketRateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);
        for (int i = 1; i <= 5; i++) {
            assertThat(tokenBucketRateLimiter.isAllowed("client1"))
                    .withFailMessage("Request %d should be allowed within bucket capacity", i)
                    .isTrue();
        }
        assertThat(tokenBucketRateLimiter.isAllowed("client1"))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();
    }

    @Test
    void givenTokenBucketRateLimiterWhenTokensRefilledGraduallyThenShouldAllowRequests() throws InterruptedException {
        tokenBucketRateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);
        String clientId = "client1";

        for (int i = 1; i <= 5; i++) {
            assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed within bucket capacity", i)
                    .isTrue();
        }
        assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();

        TimeUnit.SECONDS.sleep(2);

        assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                .withFailMessage("Request after partial refill should be allowed")
                .isTrue();
        assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                .withFailMessage("Second request after partial refill should be allowed")
                .isTrue();
        assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond available tokens should be denied")
                .isFalse();
    }

    @Test
    void givenTokenBucketRateLimiterWhenRequestsFromMultipleClientsThenShouldHandleEachClientIndependently() {
        tokenBucketRateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);

        String clientId1 = "client1";
        String clientId2 = "client2";

        for (int i = 1; i <= 5; i++) {
            assertThat(tokenBucketRateLimiter.isAllowed(clientId1))
                    .withFailMessage("Client 1 request %d should be allowed", i)
                    .isTrue();
        }
        assertThat(tokenBucketRateLimiter.isAllowed(clientId1))
                .withFailMessage("Client 1 request beyond bucket capacity should be denied")
                .isFalse();

        for (int i = 1; i <= 5; i++) {
            assertThat(tokenBucketRateLimiter.isAllowed(clientId2))
                    .withFailMessage("Client 2 request %d should be allowed", i)
                    .isTrue();
        }
    }

    @Test
    void givenTokenBucketRateLimiterWhenTokensRefillGraduallyThenShouldNotExceedBucketCapacity() throws InterruptedException {
        String clientId = "client1";
        tokenBucketRateLimiter = new TokenBucketRateLimiter(jedis, 3, 2.0);

        for (int i = 1; i <= 3; i++) {
            assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed within initial bucket capacity", i)
                    .isTrue();
        }
        assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();

        TimeUnit.SECONDS.sleep(3);

        for (int i = 1; i <= 3; i++) {
            assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed as bucket refills up to capacity", i)
                    .isTrue();
        }
        assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();
    }

    @Test
    void givenTokenBucketRateLimiterWhenRequestsAreDeniedThenShouldNotAffectTokenCount() {
        String clientId = "client1";
        tokenBucketRateLimiter = new TokenBucketRateLimiter(jedis, 3, 0.5);

        for (int i = 1; i <= 3; i++) {
            assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
        }
        assertThat(tokenBucketRateLimiter.isAllowed(clientId))
                .withFailMessage("This request should be denied")
                .isFalse();

        String key = "rate-limit:" + clientId + ":count";
        int requestCount = Integer.parseInt(jedis.get(key));
        assertThat(requestCount)
                .withFailMessage("The count should match remaining tokens and not include denied requests")
                .isEqualTo(0);
    }

}