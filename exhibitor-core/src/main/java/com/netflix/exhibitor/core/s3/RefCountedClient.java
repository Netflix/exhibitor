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

package com.netflix.exhibitor.core.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class RefCountedClient
{
    private static final int            SHUTDOWN_DELAY_MS = Integer.getInteger("RefCountedClientDelayMs", 60 * 1000); // 1 minute default

    private static class ToBeShutdown implements Delayed
    {
        private final AmazonS3Client        client;
        private final long                  endTimeMs = System.currentTimeMillis() + SHUTDOWN_DELAY_MS;

        @Override
        public long getDelay(TimeUnit unit)
        {
            return unit.convert(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o)
        {
            long            diff = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
            return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
        }

        private ToBeShutdown(AmazonS3Client client)
        {
            this.client = client;
        }
    }

    private static final DelayQueue<ToBeShutdown>  shutdownQueue = new DelayQueue<ToBeShutdown>();
    private static final ExecutorService           service = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("RefCountedClient").build());
    static
    {
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
                            shutdownQueue.take().client.shutdown();
                        }
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        );
    }

    private final AtomicBoolean markedForDelete = new AtomicBoolean(false);
    private final AmazonS3Client client;
    private final AtomicInteger useCount = new AtomicInteger(0);

    RefCountedClient(AmazonS3Client client)
    {
        this.client = client;
    }

    AmazonS3Client useClient()
    {
        useCount.incrementAndGet();
        return client;
    }

    void        markForDelete()
    {
        markedForDelete.set(true);
    }

    void        release()
    {
        if ( useCount.decrementAndGet() == 0 )
        {
            if ( markedForDelete.get() )
            {
                shutdownQueue.add(new ToBeShutdown(client));
            }
        }
    }
}
