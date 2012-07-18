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

package com.netflix.exhibitor.core.activity;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ActivityQueue implements Closeable
{
    private static final Logger log = LoggerFactory.getLogger(ActivityQueue.class);

    private final ExecutorService               service = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("ActivityQueue-%d").build());
    private final Map<QueueGroups, DelayQueue<ActivityHolder>> queues;

    private static class ActivityHolder implements Delayed
    {
        private final Activity      activity;
        private final long          endMs;

        private ActivityHolder(Activity activity, long delayMs)
        {
            this.activity = activity;
            endMs = System.currentTimeMillis() + delayMs;
        }

        @Override
        public long getDelay(TimeUnit unit)
        {
            return unit.convert(endMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        // Note: this class has a natural ordering that is inconsistent with equals
        public int compareTo(Delayed rhs)
        {
            if ( rhs == this )
            {
                return 0;
            }

            long    diff = getDelay(TimeUnit.MILLISECONDS) - rhs.getDelay(TimeUnit.MILLISECONDS);
            return (diff == 0) ? 0 : ((diff < 0) ? -1 : 1);
        }

        @Override
        public boolean equals(Object o)
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            ActivityHolder rhs = (ActivityHolder)o;
            return activity == rhs.activity;    // ID comparison on purpose
        }

        @Override
        public int hashCode()
        {
            int result = activity.hashCode();
            result = 31 * result + (int)(endMs ^ (endMs >>> 32));
            return result;
        }
    }

    public ActivityQueue()
    {
        ImmutableMap.Builder<QueueGroups, DelayQueue<ActivityHolder>>   builder = ImmutableMap.builder();
        for ( QueueGroups group : QueueGroups.values() )
        {
            builder.put(group, new DelayQueue<ActivityHolder>());
        }
        queues = builder.build();
    }

    /**
     * The queue must be started
     */
    public void start()
    {
        for ( QueueGroups group : QueueGroups.values() )
        {
            final DelayQueue<ActivityHolder>      thisQueue = queues.get(group);
            service.submit
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            while ( !Thread.currentThread().isInterrupted() )
                            {
                                ActivityHolder holder = thisQueue.take();
                                try
                                {
                                    Boolean result = holder.activity.call();
                                    holder.activity.completed((result != null) && result);
                                }
                                catch ( Throwable e )
                                {
                                    log.error("Unhandled exception in background task", e);
                                }
                            }
                        }
                        catch ( InterruptedException dummy )
                        {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            );
        }
    }

    /**
     * Add an activity to the given queue
     *
     * @param group the queue - all activities within a queue are executed serially
     * @param activity the activity
     */
    public synchronized void     add(QueueGroups group, Activity activity)
    {
        add(group, activity, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Add an activity to the given queue that executes after a specified delay
     *
     * @param group the queue - all activities within a queue are executed serially
     * @param activity the activity
     * @param delay the delay
     * @param unit the delay unit
     */
    public synchronized void     add(QueueGroups group, Activity activity, long delay, TimeUnit unit)
    {
        ActivityHolder  holder = new ActivityHolder(activity, TimeUnit.MILLISECONDS.convert(delay, unit));
        queues.get(group).offer(holder);
    }

    @Override
    public void close() throws IOException
    {
        service.shutdownNow();
    }

    /**
     * Replace the given activity in the given queue. If not in the queue, adds it to the queue.
     *
     * @param group the queue - all activities within a queue are executed serially
     * @param activity the activity
     */
    public synchronized void     replace(QueueGroups group, Activity activity)
    {
        replace(group, activity, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * Replace the given activity in the given queue. If not in the queue, adds it to the queue. The activity
     * runs after the specified delay (the delay of the previous entry, if any, is ignored)
     *
     * @param group the queue - all activities within a queue are executed serially
     * @param activity the activity
     * @param delay the delay
     * @param unit the delay unit
     */
    public synchronized void     replace(QueueGroups group, Activity activity, long delay, TimeUnit unit)
    {
        ActivityHolder  holder = new ActivityHolder(activity, TimeUnit.MILLISECONDS.convert(delay, unit));
        DelayQueue<ActivityHolder> queue = queues.get(group);
        queue.remove(holder);
        queue.offer(holder);
    }
}
