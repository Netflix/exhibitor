package com.netflix.exhibitor.core.config.azure;

import com.netflix.exhibitor.core.config.AutoManageLockArguments;
import java.util.concurrent.TimeUnit;

public class AzureConfigAutoManageLockArguments extends AutoManageLockArguments
{
    private final int settlingMs;

    public AzureConfigAutoManageLockArguments(String prefix)
    {
        super(prefix);
        settlingMs = (int)TimeUnit.SECONDS.toMillis(5); // TODO - get this right
    }

    public AzureConfigAutoManageLockArguments(String prefix, int timeoutMs, int pollingMs, int settlingMs)
    {
        super(prefix, timeoutMs, pollingMs);
        this.settlingMs = settlingMs;
    }

    public int getSettlingMs()
    {
        return settlingMs;
    }
}
