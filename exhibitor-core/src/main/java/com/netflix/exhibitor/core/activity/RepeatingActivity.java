/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.exhibitor.core.activity;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper for a delayed activity that will get re-queued after it runes
 */
public class RepeatingActivity implements Closeable
{
    private final QueueGroups   group;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final Activity      activity;
    private final AtomicLong    timePeriodMs;
    private final ActivityQueue queue;

    /**
     * @param log the log
     * @param queue the queue to add to
     * @param group the queue group
     * @param actualActivity the repeating activity
     * @param timePeriodMs the period between executions
     */
    public RepeatingActivity(final ActivityLog log, ActivityQueue queue, QueueGroups group, final Activity actualActivity, long timePeriodMs)
    {
        this.queue = queue;
        this.group = group;
        this.activity = new Activity()
        {
            @Override
            public void completed(boolean wasSuccessful)
            {
                actualActivity.completed(wasSuccessful);
            }

            @Override
            public Boolean call() throws Exception
            {
                boolean     result = false;
                if ( isStarted.get() )
                {
                    try
                    {
                        result = actualActivity.call();
                    }
                    catch ( Throwable e )
                    {
                        if ( log != null )
                        {
                            log.add(ActivityLog.Type.ERROR, String.format("Unhandled exception in repeating activity (%s) - re-queueing", actualActivity.getClass().getSimpleName()), e);
                        }
                    }
                    reQueue();
                }
                return result;
            }
        };
        this.timePeriodMs = new AtomicLong(timePeriodMs);
    }

    /**
     * Must be started
     */
    public void start()
    {
        isStarted.set(true);
        reQueue();
    }

    @Override
    public void close() throws IOException
    {
        isStarted.set(false);
    }

    /**
     * Change the time period between executions
     *
     * @param newTimePeriodMs new time period
     */
    public void setTimePeriodMs(long newTimePeriodMs)
    {
        timePeriodMs.set(newTimePeriodMs);
        queue.replace(group, activity, timePeriodMs.get(), TimeUnit.MILLISECONDS);
    }

    private void reQueue()
    {
        if ( timePeriodMs.get() > 0 )
        {
            queue.add(group, activity, timePeriodMs.get(), TimeUnit.MILLISECONDS);
        }
    }
}
