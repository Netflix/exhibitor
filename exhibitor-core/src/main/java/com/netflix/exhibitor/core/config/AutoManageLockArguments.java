package com.netflix.exhibitor.core.config;

import java.util.concurrent.TimeUnit;

public class AutoManageLockArguments
{
    private final String prefix;
    private final int timeoutMs;
    private final int pollingMs;

    public AutoManageLockArguments(String prefix)
    {
        // TODO get defaults right
        this(prefix, (int)TimeUnit.MINUTES.toMillis(5), 250);
    }

    public AutoManageLockArguments(String prefix, int timeoutMs, int pollingMs)
    {
        this.prefix = prefix;
        this.timeoutMs = timeoutMs;
        this.pollingMs = pollingMs;
    }

    public String getPrefix()
    {
        return prefix;
    }

    public int getTimeoutMs()
    {
        return timeoutMs;
    }

    public int getPollingMs()
    {
        return pollingMs;
    }
}
