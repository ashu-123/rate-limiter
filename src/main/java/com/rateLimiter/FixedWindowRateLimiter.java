package com.rateLimiter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.args.ExpiryOption;

public class FixedWindowRateLimiter {

    private final Jedis jedis;
    private final int windowSize;
    private final int limit;

    public FixedWindowRateLimiter(Jedis jedis, int windowSize, int limit) {
        this.jedis = jedis;
        this.windowSize = windowSize;
        this.limit = limit;
    }

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
