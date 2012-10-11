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

package com.netflix.exhibitor.core.automanage;

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.ActivityQueue;
import com.netflix.exhibitor.core.config.ConfigManager;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import com.netflix.exhibitor.core.state.MonitorRunningInstance;
import jsr166y.ForkJoinPool;
import org.mockito.Mockito;
import java.io.Closeable;
import java.io.IOException;

class MockExhibitorInstance implements Closeable
{
    private final Exhibitor mockExhibitor;
    private final ForkJoinPool mockForkJoinPool;
    private final MockConfigProvider mockConfigProvider;

    MockExhibitorInstance(String hostname) throws Exception
    {
        ActivityLog log = new ActivityLog(100);
        ActivityQueue activityQueue = new ActivityQueue();

        MonitorRunningInstance      monitorRunningInstance = Mockito.mock(MonitorRunningInstance.class);
        Mockito.when(monitorRunningInstance.getCurrentInstanceState()).thenReturn(InstanceStateTypes.SERVING);


        mockExhibitor = Mockito.mock(Exhibitor.class);
        Mockito.when(mockExhibitor.getLog()).thenReturn(log);
        Mockito.when(mockExhibitor.getActivityQueue()).thenReturn(activityQueue);
        Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn(hostname);
        Mockito.when(mockExhibitor.getMonitorRunningInstance()).thenReturn(monitorRunningInstance);

        mockForkJoinPool = Mockito.mock(ForkJoinPool.class);
        Mockito.when(mockExhibitor.getForkJoinPool()).thenReturn(mockForkJoinPool);

        mockConfigProvider = new MockConfigProvider();

        ConfigManager manager = new ConfigManager(mockExhibitor, mockConfigProvider, 10);
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

    ForkJoinPool getMockForkJoinPool()
    {
        return mockForkJoinPool;
    }

    MockConfigProvider getMockConfigProvider()
    {
        return mockConfigProvider;
    }
}
