/*
 * Copyright 2014 Netflix, Inc.
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

import com.google.common.collect.Queues;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.ActivityQueue;
import com.netflix.exhibitor.core.automanage.RemoteInstanceRequest;
import com.netflix.exhibitor.core.controlpanel.ControlPanelTypes;
import com.netflix.exhibitor.core.controlpanel.ControlPanelValues;
import com.netflix.exhibitor.core.processes.ProcessOperations;
import com.netflix.exhibitor.core.state.InstanceState;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import com.netflix.exhibitor.core.state.MonitorRunningInstance;
import com.netflix.exhibitor.core.state.StateAndLeader;
import org.apache.curator.utils.CloseableUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TestRollingChangeStaging
{
    @Test
    public void testMultiple() throws Exception
    {
        internalTest(3);
    }

    @Test
    public void testBasic() throws Exception
    {
        internalTest(1);
    }

    private void internalTest(final int attempts) throws Exception
    {
        final Semaphore restartLatch = new Semaphore(0);

        TestProcessOperations mockOperations1 = new TestProcessOperations();
        TestProcessOperations mockOperations2 = new TestProcessOperations();
        TestProcessOperations mockOperations3 = new TestProcessOperations();

        final InstanceConfig defaultInstanceConfig = DefaultProperties.newDefaultInstanceConfig(null);
        final InstanceConfig instanceConfig = new InstanceConfig()
        {
            @Override
            public String getString(StringConfigs config)
            {
                if ( config == StringConfigs.SERVERS_SPEC )
                {
                    return "1:one,2:two,3:three";
                }

                if ( (config == StringConfigs.ZOOKEEPER_DATA_DIRECTORY) || (config == StringConfigs.ZOOKEEPER_INSTALL_DIRECTORY) )
                {
                    return "foo";
                }

                return defaultInstanceConfig.getString(config);
            }

            @Override
            public int getInt(IntConfigs config)
            {
                if ( config == IntConfigs.CHECK_MS )
                {
                    return 1;
                }
                return defaultInstanceConfig.getInt(config);
            }
        };

        Properties properties = DefaultProperties.getFromInstanceConfig(instanceConfig);
        ConfigCollection configCollection = new PropertyBasedInstanceConfig(properties, new Properties());

        LoadedInstanceConfig loadedInstanceConfig = new LoadedInstanceConfig(configCollection, 1);
        final AtomicReference<LoadedInstanceConfig> providerConfig = new AtomicReference<LoadedInstanceConfig>(loadedInstanceConfig);
        ConfigProvider mockConfigProvider = new ConfigProvider()
        {
            @Override
            public void start() throws Exception
            {
            }

            @Override
            public LoadedInstanceConfig loadConfig() throws Exception
            {
                return providerConfig.get();
            }

            @Override
            public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareVersion) throws Exception
            {
                LoadedInstanceConfig loadedInstanceConfig = new LoadedInstanceConfig(config, compareVersion + 1);
                providerConfig.set(loadedInstanceConfig);
                return loadedInstanceConfig;
            }

            @Override
            public PseudoLock newPseudoLock() throws Exception
            {
                return new PseudoLock()
                {
                    @Override
                    public boolean lock(ActivityLog log, long maxWait, TimeUnit unit) throws Exception
                    {
                        return true;
                    }

                    @Override
                    public void unlock() throws Exception
                    {

                    }
                };
            }

            @Override
            public void close() throws IOException
            {
            }
        };

        ControlPanelValues mockControlPanelValues = Mockito.mock(ControlPanelValues.class);
        Mockito.when(mockControlPanelValues.isSet(Mockito.any(ControlPanelTypes.class))).thenReturn(true);

        ActivityQueue activityQueue = new ActivityQueue();

        final Queue<AssertionError> exceptions = Queues.newConcurrentLinkedQueue();

        Exhibitor mockExhibitor1 = Mockito.mock(Exhibitor.class, Mockito.RETURNS_MOCKS);
        Mockito.when(mockExhibitor1.getActivityQueue()).thenReturn(activityQueue);
        ConfigManager configManager1 = new TestConfigManager(mockExhibitor1, mockConfigProvider);
        Mockito.when(mockExhibitor1.getConfigManager()).thenReturn(configManager1);
        MonitorRunningInstance monitorRunningInstance1 = new MockMonitorRunningInstance(mockExhibitor1, providerConfig, "one", restartLatch, exceptions);
        Mockito.when(mockExhibitor1.getMonitorRunningInstance()).thenReturn(monitorRunningInstance1);
        Mockito.when(mockExhibitor1.getThisJVMHostname()).thenReturn("one");
        Mockito.when(mockExhibitor1.getProcessOperations()).thenReturn(mockOperations1);
        Mockito.when(mockExhibitor1.getControlPanelValues()).thenReturn(mockControlPanelValues);

        Exhibitor mockExhibitor2 = Mockito.mock(Exhibitor.class, Mockito.RETURNS_MOCKS);
        Mockito.when(mockExhibitor2.getActivityQueue()).thenReturn(activityQueue);
        ConfigManager configManager2 = new TestConfigManager(mockExhibitor2, mockConfigProvider);
        Mockito.when(mockExhibitor2.getConfigManager()).thenReturn(configManager2);
        MonitorRunningInstance monitorRunningInstance2 = new MockMonitorRunningInstance(mockExhibitor2, providerConfig, "two", restartLatch, exceptions);
        Mockito.when(mockExhibitor2.getMonitorRunningInstance()).thenReturn(monitorRunningInstance2);
        Mockito.when(mockExhibitor2.getThisJVMHostname()).thenReturn("two");
        Mockito.when(mockExhibitor2.getProcessOperations()).thenReturn(mockOperations2);
        Mockito.when(mockExhibitor2.getControlPanelValues()).thenReturn(mockControlPanelValues);

        Exhibitor mockExhibitor3 = Mockito.mock(Exhibitor.class, Mockito.RETURNS_MOCKS);
        Mockito.when(mockExhibitor3.getActivityQueue()).thenReturn(activityQueue);
        ConfigManager configManager3 = new TestConfigManager(mockExhibitor3, mockConfigProvider);
        Mockito.when(mockExhibitor3.getConfigManager()).thenReturn(configManager3);
        MonitorRunningInstance monitorRunningInstance3 = new MockMonitorRunningInstance(mockExhibitor3, providerConfig, "three", restartLatch, exceptions);
        Mockito.when(mockExhibitor3.getMonitorRunningInstance()).thenReturn(monitorRunningInstance3);
        Mockito.when(mockExhibitor3.getThisJVMHostname()).thenReturn("three");
        Mockito.when(mockExhibitor3.getProcessOperations()).thenReturn(mockOperations3);
        Mockito.when(mockExhibitor3.getControlPanelValues()).thenReturn(mockControlPanelValues);

        try
        {
            activityQueue.start();

            configManager1.start();
            configManager2.start();
            configManager3.start();

            monitorRunningInstance1.start();
            monitorRunningInstance2.start();
            monitorRunningInstance3.start();

            Thread.sleep(1000);

            for ( int i = 0; i < attempts; ++i )
            {
                Assert.assertEquals(restartLatch.availablePermits(), 0);

                final int index = i;
                InstanceConfig changedInstanceConfig = new InstanceConfig()
                {
                    @Override
                    public String getString(StringConfigs config)
                    {
                        if ( config == StringConfigs.LOG4J_PROPERTIES )
                        {
                            return "something different " + index;
                        }
                        return instanceConfig.getString(config);
                    }

                    @Override
                    public int getInt(IntConfigs config)
                    {
                        return instanceConfig.getInt(config);
                    }
                };
                configManager1.startRollingConfig(changedInstanceConfig, "one");

                Assert.assertTrue(restartLatch.tryAcquire(1, 10, TimeUnit.SECONDS));

                if ( exceptions.size() > 0 )
                {
                    for ( AssertionError assertionError : exceptions )
                    {
                        assertionError.printStackTrace();
                    }
                    Assert.fail("Failed restart assertions");
                }
            }
        }
        finally
        {
            CloseableUtils.closeQuietly(monitorRunningInstance3);
            CloseableUtils.closeQuietly(monitorRunningInstance2);
            CloseableUtils.closeQuietly(monitorRunningInstance1);
            CloseableUtils.closeQuietly(configManager3);
            CloseableUtils.closeQuietly(configManager2);
            CloseableUtils.closeQuietly(configManager1);
            CloseableUtils.closeQuietly(activityQueue);
        }
    }

    private static class TestProcessOperations implements ProcessOperations
    {
        private final AtomicLong lastStartMs = new AtomicLong();

        @Override
        public void startInstance() throws Exception
        {
            lastStartMs.set(System.currentTimeMillis());
            Thread.sleep(1000);
        }

        @Override
        public void killInstance() throws Exception
        {
        }

        @Override
        public void cleanupInstance() throws Exception
        {
        }
    }

    private static class TestConfigManager extends ConfigManager
    {
        public TestConfigManager(Exhibitor mockExhibitor, ConfigProvider mockConfigProvider) throws Exception
        {
            super(mockExhibitor, mockConfigProvider, 1);
        }

        @Override
        protected RemoteInstanceRequest.Result callRemoteInstanceRequest(RemoteInstanceRequest remoteInstanceRequest)
        {
            return new RemoteInstanceRequest.Result("OK", "");
        }
    }

    private class MockMonitorRunningInstance extends MonitorRunningInstance
    {
        private final AtomicReference<LoadedInstanceConfig> providerConfig;
        private final String hostname;
        private final Semaphore restartLatch;
        private final Queue<AssertionError> exceptions;
        private volatile StateAndLeader stateAndLeader;

        public MockMonitorRunningInstance(Exhibitor mockExhibitor, AtomicReference<LoadedInstanceConfig> providerConfig, String hostname, Semaphore restartLatch, Queue<AssertionError> exceptions)
        {
            super(mockExhibitor);
            this.providerConfig = providerConfig;
            this.hostname = hostname;
            this.restartLatch = restartLatch;
            this.exceptions = exceptions;
            stateAndLeader = new StateAndLeader(InstanceStateTypes.SERVING, hostname.equals("one"));
        }

        @Override
        protected StateAndLeader getStateAndLeader() throws Exception
        {
            return stateAndLeader;
        }

        @Override
        protected void restartZooKeeper(InstanceState currentInstanceState) throws Exception
        {
            try
            {
                Assert.assertTrue(providerConfig.get().getConfig().isRolling());
                String rollingHostName = providerConfig.get().getConfig().getRollingConfigState().getRollingHostNames().get(providerConfig.get().getConfig().getRollingConfigState().getRollingHostNamesIndex());
                Assert.assertEquals(rollingHostName, hostname);

                stateAndLeader = new StateAndLeader(InstanceStateTypes.DOWN, stateAndLeader.isLeader());

                Executors.newSingleThreadScheduledExecutor().schedule
                (
                    new Runnable()
                    {
                        @Override
                        public void run()
                        {
                            stateAndLeader = new StateAndLeader(InstanceStateTypes.SERVING, stateAndLeader.isLeader());
                        }
                    },
                    10,
                    TimeUnit.MILLISECONDS
                );
            }
            catch ( AssertionError e )
            {
                exceptions.add(e);
            }
            finally
            {
                restartLatch.release();
            }
        }
    }
}
