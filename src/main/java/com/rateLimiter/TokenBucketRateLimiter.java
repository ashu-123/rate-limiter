package com.rateLimiter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/** This class implements token bucket rate limiting algorithm using Jedis.*/
public class TokenBucketRateLimiter {

    private final Jedis jedis;
    private final int maxBucketCapacity;
    private final double refillRate;

    public TokenBucketRateLimiter(Jedis jedis, int maxBucketCapacity, double refillRate) {
        this.jedis = jedis;
        this.maxBucketCapacity = maxBucketCapacity;
        this.refillRate = refillRate;
    }

    /**
     * Checks if a client is within their allowed request limit.
     * If yes, the request is allowed. If not, the request is blocked.
     * @param clientId the unique id of the client
     * @return True if request is allowed. False if not.
     */
    public boolean isAllowed(String clientId) {
        String keyCount = "rate-limit:" + clientId +":count";
        String keyLastRefill = "rate-limit:" + clientId +":lastRefill";

        Transaction transaction = jedis.multi();
        transaction.get(keyLastRefill);
        transaction.get(keyCount);
        var results = transaction.exec();

        long currentTime = System.currentTimeMillis();
        long lastRefillTime = results.get(0)!=null?Long.parseLong((String)results.get(0)):currentTime;
        int tokenCount = results.get(1)!=null?Integer.parseInt((String) results.get(1)):maxBucketCapacity;

        long elapsedTime = currentTime - lastRefillTime;
        double elapsedTimeInSeconds = elapsedTime/1000.0;
        int tokensToAdd = (int)(elapsedTimeInSeconds*refillRate);

        tokenCount = Math.min(maxBucketCapacity, tokenCount+tokensToAdd);
        boolean isAllowed = tokenCount>0;
        if(isAllowed) {
            tokenCount--;
        }
        transaction = jedis.multi();
        transaction.set(keyCount, String.valueOf(tokenCount));
        transaction.set(keyLastRefill, String.valueOf(currentTime));
        transaction.exec();

        return isAllowed;
    }
}
