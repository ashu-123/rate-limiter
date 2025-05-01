package com.rateLimiter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.args.ExpiryOption;

/** This class implements a fixed window size rate limiting algorithm using Jedis.*/
public class FixedWindowRateLimiter {

    private final Jedis jedis;
    private final int windowSize;
    private final int limit;

    public FixedWindowRateLimiter(Jedis jedis, int windowSize, int limit) {
        this.jedis = jedis;
        this.windowSize = windowSize;
        this.limit = limit;
    }

    /**
     * Checks if a client is within their allowed request limit.
     * If yes, the request is allowed and the counter is updated. If not, the request is blocked.
     * @param clientId the unique id of the client
     * @return True if request is allowed. False if not.
     */
    public boolean isAllowed(String clientId) {
        String key = "rate-limit: " + clientId;

        String currentCountVal = jedis.get(key);
        int currentCount = currentCountVal!=null?Integer.parseInt(currentCountVal):0;

        boolean isAllowed = currentCount<limit;

        if(isAllowed) {
            Transaction transaction = jedis.multi();
            transaction.incr(key);
            transaction.expire(key, windowSize, ExpiryOption.NX);
            transaction.exec();
        }

        return isAllowed;
    }
}
