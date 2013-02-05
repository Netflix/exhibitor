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

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class TestActivityQueue
{
    @Test
    public void     test() throws Exception
    {
        final CountDownLatch      latch = new CountDownLatch(2);
        final ReentrantLock       lock = new ReentrantLock();
        Thread                    t1 = new Thread
        (

            new Runnable()
            {
                @Override
                public void run()
                {
                    lock.lock();
                    latch.countDown();
                }
            }
        );
        Thread                    t2 = new Thread
        (

            new Runnable()
            {
                @Override
                public void run()
                {
                    lock.lock();
                    latch.countDown();
                }
            }
        );

        t1.start();
        t2.start();
        if ( latch.await(10, TimeUnit.SECONDS) )
        {
            System.out.println("yep");
        }
        else
        {
            System.out.println("nope");
        }
    }
    
    @Test
    public void testSequential() throws Exception
    {
        ActivityQueue queue = new ActivityQueue();
        queue.start();
        try
        {
            final CountDownLatch    latch = new CountDownLatch(1);
            Activity                activity1 = new Activity()
            {
                @Override
                public void completed(boolean wasSuccessful)
                {
                }

                @Override
                public Boolean call() throws Exception
                {
                    latch.await();
                    return true;
                }
            };

            final AtomicBoolean     active = new AtomicBoolean(false);
            Activity                activity2 = new Activity()
            {
                @Override
                public void completed(boolean wasSuccessful)
                {
                }

                @Override
                public Boolean call() throws Exception
                {
                    active.set(true);
                    return true;
                }
            };

            queue.add(QueueGroups.MAIN, activity1);
            queue.add(QueueGroups.MAIN, activity2);
            for ( int i = 0; i < 10; ++i )
            {
                Assert.assertFalse(active.get());
                Thread.sleep(100);
            }

            queue.add(QueueGroups.IO, activity2);
            for ( int i = 0; i < 10; ++i )
            {
                if ( active.get() )
                {
                    break;
                }
                Thread.sleep(100);
            }
            Assert.assertTrue(active.get());
        }
        finally
        {
            Closeables.closeQuietly(queue);
        }
    }

    @Test
    public void testReplace() throws Exception
    {
        ActivityQueue queue = new ActivityQueue();
        queue.start();
        try
        {
            final AtomicInteger     count = new AtomicInteger(0);
            final CountDownLatch    latch = new CountDownLatch(1);
            Activity                activity = new Activity()
            {
                @Override
                public void completed(boolean wasSuccessful)
                {
                }

                @Override
                public Boolean call() throws Exception
                {
                    count.incrementAndGet();
                    latch.countDown();
                    return true;
                }
            };

            queue.add(QueueGroups.MAIN, activity, 1, TimeUnit.MINUTES);
            queue.replace(QueueGroups.MAIN, activity);
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(count.get(), 1);
        }
        finally
        {
            Closeables.closeQuietly(queue);
        }
    }

    @Test
    public void testRepeating() throws Exception
    {
        final int DELAY = 500;

        RepeatingActivity       repeating = null;
        ActivityQueue           queue = new ActivityQueue();
        queue.start();
        try
        {
            final List<Long>        times = Lists.newArrayList();
            final CountDownLatch    latch = new CountDownLatch(3);
            Activity                activity = new Activity()
            {
                @Override
                public void completed(boolean wasSuccessful)
                {
                }

                @Override
                public Boolean call() throws Exception
                {
                    times.add(System.currentTimeMillis());
                    latch.countDown();
                    return true;
                }
            };
            repeating = new RepeatingActivityImpl(null, queue, QueueGroups.MAIN, activity, DELAY);
            repeating.start();

            long                    start = System.currentTimeMillis();
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
            repeating.close();

            Assert.assertTrue(times.size() >= 3);

            long        check = start;
            for ( int i = 0; i < 3; ++i )
            {
                long thisTime = times.get(i);
                long elapsed = thisTime - check;
                Assert.assertTrue(elapsed >= (DELAY - (DELAY / 10)), "elapsed: " + elapsed);
                check = thisTime;
            }
        }
        finally
        {
            Closeables.closeQuietly(repeating);
            Closeables.closeQuietly(queue);
        }
    }
    
    @Test
    public void testDelay() throws Exception
    {
        ActivityQueue queue = new ActivityQueue();
        queue.start();
        try
        {
            final AtomicLong        callTime = new AtomicLong();
            final CountDownLatch    latch = new CountDownLatch(1);
            Activity                activity = new Activity()
            {
                @Override
                public void completed(boolean wasSuccessful)
                {
                }

                @Override
                public Boolean call() throws Exception
                {
                    callTime.set(System.currentTimeMillis());
                    latch.countDown();
                    return true;
                }
            };

            long                    start = System.currentTimeMillis();
            queue.add(QueueGroups.MAIN, activity, 2, TimeUnit.SECONDS);
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue((callTime.get() - start) >= 2000);
        }
        finally
        {
            Closeables.closeQuietly(queue);
        }
    }

    @Test
    public void testBasic() throws Exception
    {
        ActivityQueue queue = new ActivityQueue();
        queue.start();
        try
        {
            final CountDownLatch    latch = new CountDownLatch(2);
            Activity                activity = new Activity()
            {
                @Override
                public void completed(boolean wasSuccessful)
                {
                    latch.countDown();
                }

                @Override
                public Boolean call() throws Exception
                {
                    latch.countDown();
                    return true;
                }
            };

            queue.add(QueueGroups.MAIN, activity);
            Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
        }
        finally
        {
            Closeables.closeQuietly(queue);
        }
    }
}
