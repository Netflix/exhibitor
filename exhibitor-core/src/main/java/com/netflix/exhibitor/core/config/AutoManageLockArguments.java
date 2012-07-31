/*
 * Copyright 2012 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
        this(prefix, (int)TimeUnit.MINUTES.toMillis(15), 250);
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
