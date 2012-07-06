package com.netflix.exhibitor.core.config.s3;

import com.netflix.exhibitor.core.config.AutoManageLockArguments;
import java.util.concurrent.TimeUnit;

public class S3ConfigAutoManageLockArguments extends AutoManageLockArguments
{
    private final int settlingMs;

    public S3ConfigAutoManageLockArguments(String prefix)
    {
        super(prefix);
        settlingMs = (int)TimeUnit.SECONDS.toMillis(5); // TODO - get this right
    }

    public S3ConfigAutoManageLockArguments(String prefix, int timeoutMs, int pollingMs, int settlingMs)
    {
        super(prefix, timeoutMs, pollingMs);
        this.settlingMs = settlingMs;
    }

    public int getSettlingMs()
    {
        return settlingMs;
    }
}
