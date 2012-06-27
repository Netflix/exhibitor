package com.netflix.exhibitor.core.config.s3;

import java.util.concurrent.TimeUnit;

public class S3ConfigAutoManageLockArguments
{
    private final String prefix;
    private final int timeoutMs;
    private final int pollingMs;
    private final int settlingMs;

    public S3ConfigAutoManageLockArguments(String prefix)
    {
        // TODO get defaults right
        this
        (
            prefix,
            (int)TimeUnit.MINUTES.toMillis(1),
            250,
            (int)TimeUnit.SECONDS.toMillis(5)
        );
    }

    public S3ConfigAutoManageLockArguments(String prefix, int timeoutMs, int pollingMs, int settlingMs)
    {
        this.prefix = prefix;
        this.timeoutMs = timeoutMs;
        this.pollingMs = pollingMs;
        this.settlingMs = settlingMs;
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

    public int getSettlingMs()
    {
        return settlingMs;
    }
}
