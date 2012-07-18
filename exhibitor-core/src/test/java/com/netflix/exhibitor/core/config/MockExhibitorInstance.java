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

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.ActivityQueue;
import org.mockito.Mockito;
import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

class MockExhibitorInstance implements Closeable
{
    private final Exhibitor mockExhibitor;

    MockExhibitorInstance(String hostname) throws Exception
    {
        this(hostname, getConfigProvider());
    }

    MockExhibitorInstance(String hostname, ConfigProvider provider) throws Exception
    {
        ActivityLog log = new ActivityLog(100);
        ActivityQueue activityQueue = new ActivityQueue();
        mockExhibitor = Mockito.mock(Exhibitor.class);
        Mockito.when(mockExhibitor.getLog()).thenReturn(log);
        Mockito.when(mockExhibitor.getActivityQueue()).thenReturn(activityQueue);
        Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn(hostname);

        ConfigManager       manager = new ConfigManager(mockExhibitor, provider, 10);
        manager.start();

        Mockito.when(mockExhibitor.getConfigManager()).thenReturn(manager);
    }

    @Override
    public void close() throws IOException
    {
        mockExhibitor.getConfigManager().close();
    }

    Exhibitor getMockExhibitor()
    {
        return mockExhibitor;
    }

    private static ConfigProvider getConfigProvider()
    {
        return new ConfigProvider()
        {
            private volatile ConfigCollection       config = new PropertyBasedInstanceConfig(new Properties(), new Properties());
            private final AtomicLong                modified = new AtomicLong(1);

            @Override
            public LoadedInstanceConfig loadConfig() throws Exception
            {
                return new LoadedInstanceConfig(config, modified.get());
            }

            @Override
            public void writeInstanceHeartbeat(String instanceHostname) throws Exception
            {
            }

            @Override
            public void clearInstanceHeartbeat(String instanceHostname) throws Exception
            {
            }

            @Override
            public long getLastHeartbeatForInstance(String instanceHostname) throws Exception
            {
                return 0;
            }

            @Override
            public PseudoLock newPseudoLock() throws Exception
            {
                return null;
            }

            @Override
            public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareLastModified) throws Exception
            {
                this.config = config;
                modified.incrementAndGet();
                return loadConfig();
            }
        };
    }
}
