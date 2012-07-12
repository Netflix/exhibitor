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

import com.google.common.io.Files;
import com.netflix.curator.test.DirectoryUtils;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.config.filesystem.FileSystemPseudoLock;
import com.netflix.exhibitor.core.state.AutomaticInstanceManagement;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import com.netflix.exhibitor.core.state.MonitorRunningInstance;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestAutoInstanceManagement
{
    @Test
    public void     testContentionAddNewInstance() throws Exception
    {
        String                      name = PropertyBasedInstanceConfig.toName(IntConfigs.AUTO_MANAGE_INSTANCES, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX);
        Properties                  properties = new Properties();
        properties.setProperty(name, "1");
        final ConfigCollection      config = new PropertyBasedInstanceConfig(properties, new Properties());
        final File                  tempDirectory = Files.createTempDir();

        final AtomicBoolean         management1DidWork = new AtomicBoolean(false);
        final AtomicBoolean         management2DidWork = new AtomicBoolean(false);
        final CountDownLatch        lockAttemptsLatch = new CountDownLatch(2);
        final CountDownLatch        isLockedLatch = new CountDownLatch(1);
        final CountDownLatch        management2DoneLatch = new CountDownLatch(1);
        final CountDownLatch        canContinueLatch = new CountDownLatch(1);
        try
        {
            ConfigProvider              configProvider = new ConfigProvider()
            {
                private volatile LoadedInstanceConfig loadedInstanceConfig;

                @Override
                public LoadedInstanceConfig loadConfig() throws Exception
                {
                    loadedInstanceConfig = new LoadedInstanceConfig(config, 0);
                    return loadedInstanceConfig;
                }

                @Override
                public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareLastModified) throws Exception
                {
                    loadedInstanceConfig = new LoadedInstanceConfig(config, 0);
                    return loadedInstanceConfig;
                }

                @Override
                public void writeInstanceHeartbeat(String instanceHostname) throws Exception
                {
                }

                @Override
                public long getLastHeartbeatForInstance(String instanceHostname) throws Exception
                {
                    return System.currentTimeMillis();
                }

                @Override
                public PseudoLock newPseudoLock() throws Exception
                {
                    return new FileSystemPseudoLock(tempDirectory, "exhibitor-lock", 600000, 1000, 0)
                    {
                        @Override
                        public boolean lock(long maxWait, TimeUnit unit) throws Exception
                        {
                            lockAttemptsLatch.countDown();
                            boolean locked = super.lock(maxWait, unit);
                            isLockedLatch.countDown();
                            return locked;
                        }

                        @Override
                        public void unlock() throws Exception
                        {
                            canContinueLatch.await();
                            super.unlock();
                        }
                    };
                }
            };

            MockExhibitorInstance           exhibitorInstance = new MockExhibitorInstance("test", configProvider);
            Exhibitor                       exhibitor = exhibitorInstance.getMockExhibitor();

            MonitorRunningInstance          monitorRunningInstance = Mockito.mock(MonitorRunningInstance.class);
            Mockito.when(monitorRunningInstance.getCurrentInstanceState()).thenReturn(InstanceStateTypes.NOT_SERVING);
            Mockito.when(exhibitor.getMonitorRunningInstance()).thenReturn(monitorRunningInstance);

            final AutomaticInstanceManagement management1 = new AutomaticInstanceManagement(exhibitor)
            {
                @Override
                protected void doWork() throws Exception
                {
                    management1DidWork.set(true);
                    super.doWork();
                }
            };
            Executors.newSingleThreadExecutor().submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        management1.call();
                        return null;
                    }
                }
            );
            Assert.assertTrue(isLockedLatch.await(5, TimeUnit.SECONDS));

            final AutomaticInstanceManagement management2 = new AutomaticInstanceManagement(exhibitor)
            {
                @Override
                protected void doWork() throws Exception
                {
                    management2DidWork.set(true);
                    super.doWork();
                }
            };
            Executors.newSingleThreadExecutor().submit
            (
                new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        management2.call();
                        management2DoneLatch.countDown();
                        return null;
                    }
                }
            );
            Assert.assertTrue(lockAttemptsLatch.await(5, TimeUnit.SECONDS));
            canContinueLatch.countDown();

            Assert.assertTrue(management2DoneLatch.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(management1DidWork.get());
            Assert.assertFalse(management2DidWork.get());
        }
        finally
        {
            DirectoryUtils.deleteRecursively(tempDirectory);
        }
    }

    @Test
    public void     testSimpleAddNewInstance() throws Exception
    {
        String                      name = PropertyBasedInstanceConfig.toName(IntConfigs.AUTO_MANAGE_INSTANCES, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX);
        Properties                  properties = new Properties();
        properties.setProperty(name, "1");
        final ConfigCollection      config = new PropertyBasedInstanceConfig(properties, new Properties());
        final File                  tempDirectory = Files.createTempDir();

        try
        {
            ConfigProvider              configProvider = new ConfigProvider()
            {
                @Override
                public LoadedInstanceConfig loadConfig() throws Exception
                {
                    return new LoadedInstanceConfig(config, 0);
                }

                @Override
                public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareLastModified) throws Exception
                {
                    return new LoadedInstanceConfig(config, 0);
                }

                @Override
                public void writeInstanceHeartbeat(String instanceHostname) throws Exception
                {
                }

                @Override
                public long getLastHeartbeatForInstance(String instanceHostname) throws Exception
                {
                    return System.currentTimeMillis();
                }

                @Override
                public PseudoLock newPseudoLock() throws Exception
                {
                    return new FileSystemPseudoLock(tempDirectory, "exhibitor-lock", 600000, 1000, 0);
                }
            };

            MockExhibitorInstance       exhibitorInstance = new MockExhibitorInstance("test", configProvider);
            Exhibitor                   exhibitor = exhibitorInstance.getMockExhibitor();

            MonitorRunningInstance      monitorRunningInstance = Mockito.mock(MonitorRunningInstance.class);
            Mockito.when(monitorRunningInstance.getCurrentInstanceState()).thenReturn(InstanceStateTypes.NOT_SERVING);
            Mockito.when(exhibitor.getMonitorRunningInstance()).thenReturn(monitorRunningInstance);

            AutomaticInstanceManagement management = new AutomaticInstanceManagement(exhibitor);
            management.call();
        }
        finally
        {
            DirectoryUtils.deleteRecursively(tempDirectory);
        }
    }

    @Test
    public void     testSimpleRemoveInstance() throws Exception
    {
        Properties                  properties = new Properties();
        properties.setProperty(PropertyBasedInstanceConfig.toName(IntConfigs.AUTO_MANAGE_INSTANCES, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), "1");
        properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.SERVERS_SPEC, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), "1:test,2:dead");

        final ConfigCollection      config = new PropertyBasedInstanceConfig(properties, new Properties());
        final File                  tempDirectory = Files.createTempDir();

        final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(1);
        try
        {
            ConfigProvider              configProvider = new ConfigProvider()
            {
                @Override
                public LoadedInstanceConfig loadConfig() throws Exception
                {
                    return new LoadedInstanceConfig(config, 0);
                }

                @Override
                public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareLastModified) throws Exception
                {
                    queue.put(config.getRollingConfig().getString(StringConfigs.SERVERS_SPEC));
                    return new LoadedInstanceConfig(config, 0);
                }

                @Override
                public void writeInstanceHeartbeat(String instanceHostname) throws Exception
                {
                }

                @Override
                public long getLastHeartbeatForInstance(String instanceHostname) throws Exception
                {
                    return instanceHostname.equals("dead") ? 1 : System.currentTimeMillis();    // zero is treated specially to mean no heartbeat yet
                }

                @Override
                public PseudoLock newPseudoLock() throws Exception
                {
                    return new FileSystemPseudoLock(tempDirectory, "exhibitor-lock", 600000, 1000, 0);
                }
            };

            MockExhibitorInstance       exhibitorInstance = new MockExhibitorInstance("test", configProvider);
            Exhibitor                   exhibitor = exhibitorInstance.getMockExhibitor();

            MonitorRunningInstance      monitorRunningInstance = Mockito.mock(MonitorRunningInstance.class);
            Mockito.when(monitorRunningInstance.getCurrentInstanceState()).thenReturn(InstanceStateTypes.NOT_SERVING);
            Mockito.when(exhibitor.getMonitorRunningInstance()).thenReturn(monitorRunningInstance);

            AutomaticInstanceManagement management = new AutomaticInstanceManagement(exhibitor, 1);
            management.call();
        }
        finally
        {
            DirectoryUtils.deleteRecursively(tempDirectory);
        }

        String newServersList = queue.poll(5, TimeUnit.SECONDS);
        Assert.assertEquals("1:test", newServersList);
    }
}
