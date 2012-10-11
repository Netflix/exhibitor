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

import com.netflix.exhibitor.core.activity.ActivityLog;
import java.util.concurrent.TimeUnit;

public interface PseudoLock
{
    /**
     * Acquire the lock, blocking at most <code>maxWait</code> until it is acquired
     *
     * @param log the logger
     * @param maxWait max time to wait
     * @param unit time unit
     * @return true if the lock was acquired
     * @throws Exception errors
     */
    public boolean lock(ActivityLog log, long maxWait, TimeUnit unit) throws Exception;

    /**
     * Release the lock
     *
     * @throws Exception errors
     */
    public void unlock() throws Exception;
}
