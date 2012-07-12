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
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestRefCountedClient
{
    @Test
    public void     testBasic() throws Exception
    {
        System.setProperty("RefCountedClientDelayMs", "1");

        final CountDownLatch    shutdownLatch = new CountDownLatch(1);
        ExecutorService         service = Executors.newSingleThreadExecutor();
        Future<Object>          future = service.submit
        (
            new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    RefCountedClient        client = new RefCountedClient(new MockRef(shutdownLatch));
                    Assert.assertNotNull(client.useClient());
                    client.release();

                    Assert.assertFalse(shutdownLatch.await(5, TimeUnit.SECONDS));

                    client.markForDelete();
                    client.useClient();
                    client.release();

                    return null;
                }
            }
        );

        future.get();
        if ( shutdownLatch.await(5, TimeUnit.SECONDS) )
        {
            return;
        }
        Assert.fail();
    }

    private static class MockRef extends AmazonS3Client
    {
        private final CountDownLatch shutdownLatch;

        public MockRef(CountDownLatch shutdownLatch)
        {
            this.shutdownLatch = shutdownLatch;
        }

        @Override
        public void shutdown()
        {
            shutdownLatch.countDown();
        }
    }
}
