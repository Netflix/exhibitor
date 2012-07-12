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
