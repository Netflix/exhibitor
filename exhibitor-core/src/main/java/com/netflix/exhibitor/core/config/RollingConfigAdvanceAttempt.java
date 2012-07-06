package com.netflix.exhibitor.core.config;

import java.util.concurrent.atomic.AtomicInteger;

class RollingConfigAdvanceAttempt
{
    private final String    hostname;
    private AtomicInteger   attemptCount = new AtomicInteger(0);

    RollingConfigAdvanceAttempt(String hostname)
    {
        this.hostname = hostname;
    }

    String getHostname()
    {
        return hostname;
    }

    int getAttemptCount()
    {
        return attemptCount.get();
    }

    void incrementAttemptCount()
    {
        attemptCount.incrementAndGet();
    }
}
