package com.rateLimiter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.args.ExpiryOption;

import java.util.Map;

/** This class implements sliding window rate limiting algorithm using Jedis.*/
public class SlidingWindowRateLimiter {

    private final Jedis jedis;
    private final long windowSize;
    private final long subWindowSize;
    private final int limit;

    public SlidingWindowRateLimiter(Jedis jedis, long windowSize, long subWindowSize, int limit) {
        this.jedis = jedis;
        this.windowSize = windowSize;
        this.subWindowSize = subWindowSize;
        this.limit = limit;
    }

    /**
     * Checks if a client is within their allowed request limit.
     * If yes, the request is allowed and the counter is updated. If not, the request is blocked.
     * @param clientId the unique id of the client
     * @return True if request is allowed. False if not.
     */
    public boolean isAllowed(String clientId) {
        String key = "rate-limiter: " + clientId;
        Map<String, String> subWindowCounts = jedis.hgetAll(key);
        Long totalRequestCount = subWindowCounts.values()
                .stream()
                .mapToLong(Long::parseLong)
                .sum();

        boolean isAllowed = totalRequestCount<limit;
        if(isAllowed) {
            Long currentTimeInMilliseconds = System.currentTimeMillis();
            Long subWindowSizeInMilliseconds = subWindowSize*1000;
            Long currentSubWindow = currentTimeInMilliseconds/subWindowSizeInMilliseconds;

            Transaction transaction = jedis.multi();
            transaction.hincrBy(key, String.valueOf(currentSubWindow), 1);
            transaction.hexpire(key, windowSize, ExpiryOption.NX, String.valueOf(currentSubWindow));

            var result = transaction.exec();
            if(result==null || result.isEmpty()) {
                throw new IllegalStateException("Empty result from Redis");
            }
        }

        return isAllowed;
    }
}
