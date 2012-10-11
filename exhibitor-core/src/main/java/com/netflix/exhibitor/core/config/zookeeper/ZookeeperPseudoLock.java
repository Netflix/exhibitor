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

package com.netflix.exhibitor.core.config.zookeeper;

import com.netflix.curator.framework.recipes.locks.InterProcessLock;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.PseudoLock;
import java.util.concurrent.TimeUnit;

public class ZookeeperPseudoLock implements PseudoLock
{
    private final InterProcessLock lock;

    public ZookeeperPseudoLock(InterProcessLock lock)
    {
        this.lock = lock;
    }

    @Override
    public boolean lock(ActivityLog log, long maxWait, TimeUnit unit) throws Exception
    {
        boolean acquire = lock.acquire(maxWait, unit);
        if ( !acquire )
        {
            log.add(ActivityLog.Type.ERROR, String.format("Could not acquire lock within %d ms", unit.toMillis(maxWait)));
        }
        return acquire;
    }

    @Override
    public void unlock() throws Exception
    {
        lock.release();
    }
}
