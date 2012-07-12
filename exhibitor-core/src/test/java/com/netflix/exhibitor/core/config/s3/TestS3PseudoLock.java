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

import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.backup.s3.MockS3Client;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestS3PseudoLock
{
    @Test
    public void         testBlocking() throws Exception
    {
        final int       QTY = 5;
        final int       POLLING_MS = 1;

        final AtomicBoolean             isLocked = new AtomicBoolean(false);
        final AtomicInteger             lockCount = new AtomicInteger(0);

        final MockS3Client              client = new MockS3Client(null, null);

        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<Void>(Executors.newFixedThreadPool(QTY));
        for ( int i = 0; i < QTY; ++i )
        {
            completionService.submit
            (
                new Callable<Void>()
                {
                    @Override
                    public Void call() throws Exception
                    {
                        S3PseudoLock lock = new S3PseudoLock(client, "foo", "bar", Integer.MAX_VALUE, POLLING_MS, 0);
                        try
                        {
                            Assert.assertTrue(lock.lock(10, TimeUnit.SECONDS));
                            Assert.assertTrue(isLocked.compareAndSet(false, true));
                            lockCount.incrementAndGet();
                            Thread.sleep(POLLING_MS);
                        }
                        finally
                        {
                            if ( isLocked.compareAndSet(true, false) )
                            {
                                lock.unlock();
                            }
                        }
                        return null;
                    }
                }
            );
            Thread.sleep(1);
        }

        for ( int i = 0; i < QTY; ++i )
        {
            completionService.take().get();
        }

        Assert.assertEquals(lockCount.get(), QTY);
    }

    @Test
    public void         testMulti() throws Exception
    {
        final int       QTY = 5;
        final int       POLLING_MS = 1;

        List<S3PseudoLock>  locks = Lists.newArrayList();
        for ( int i = 0; i < QTY; ++i )
        {
            MockS3Client        client = new MockS3Client();
            S3PseudoLock        lock = new S3PseudoLock(client, "foo", "-prefix-", 10000, POLLING_MS, 0);
            locks.add(lock);
        }

        for ( S3PseudoLock lock : locks )
        {
            Assert.assertTrue(lock.lock(5, TimeUnit.SECONDS));
            try
            {
                //noinspection PointlessArithmeticExpression
                Thread.sleep(POLLING_MS * 2);
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    @Test
    public void         testSingle() throws Exception
    {
        MockS3Client        client = new MockS3Client();

        S3PseudoLock        lock = new S3PseudoLock(client, "foo", "bar", 10000, 1, 0);
        Assert.assertTrue(lock.lock(5, TimeUnit.SECONDS));
        lock.unlock();
    }
}
