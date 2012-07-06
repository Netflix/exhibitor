package com.netflix.exhibitor.core.config;

import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.ActivityQueue;
import com.netflix.exhibitor.core.state.InstanceState;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import com.netflix.exhibitor.core.state.RemoteInstanceRequestClient;
import com.netflix.exhibitor.core.state.RestartSignificantConfig;
import com.netflix.exhibitor.core.state.ServerList;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class TestRollingConfigChange
{
    @Test
    public void testAllDownInstances() throws Exception
    {
        ServerList          serverList = new ServerList("1:one,2:two,3:three");

        RemoteInstanceRequestClient     mockClient = new RemoteInstanceRequestClient()
        {
            @Override
            public <T> T getWebResource(URI remoteUri, MediaType type, Class<T> clazz) throws Exception
            {
                throw new Exception();
            }
        };

        ActivityLog         log = new ActivityLog(100);
        ActivityQueue       activityQueue = new ActivityQueue();
        Exhibitor           mockExhibitor = Mockito.mock(Exhibitor.class);
        Mockito.when(mockExhibitor.getLog()).thenReturn(log);
        Mockito.when(mockExhibitor.getActivityQueue()).thenReturn(activityQueue);
        Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn("_xxxx_");
        Mockito.when(mockExhibitor.getRemoteInstanceRequestClient()).thenReturn(mockClient);

        final AtomicLong    modified = new AtomicLong(1);
        ConfigProvider      provider = new ConfigProvider()
        {
            private volatile ConfigCollection      config = new PropertyBasedInstanceConfig(new Properties(), new Properties());

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

        ConfigManager       manager = new ConfigManager(mockExhibitor, provider, 10, 1);
        manager.start();
        try
        {
            Properties                      properties = new Properties();
            properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.SERVERS_SPEC, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), serverList.toSpecString());
            PropertyBasedInstanceConfig     config = new PropertyBasedInstanceConfig(properties, DefaultProperties.get(null));
            manager.startRollingConfig(config.getRootConfig());

            Assert.assertFalse(manager.isRolling());
        }
        finally
        {
            Closeables.closeQuietly(manager);
        }
    }

    @Test
    public void testDownMiddleInstance() throws Exception
    {
        ServerList          serverList = new ServerList("1:aaa,2:two,3:zzz");

        RemoteInstanceRequestClient     mockClient = new RemoteInstanceRequestClient()
        {
            @Override
            public <T> T getWebResource(URI remoteUri, MediaType type, Class<T> clazz) throws Exception
            {
                if ( remoteUri.getHost().equals("two") )
                {
                    throw new Exception();
                }
                return clazz.cast("foo");
            }
        };

        ActivityLog         log = new ActivityLog(100);
        ActivityQueue       activityQueue = new ActivityQueue();
        Exhibitor           mockExhibitor = Mockito.mock(Exhibitor.class);
        Mockito.when(mockExhibitor.getLog()).thenReturn(log);
        Mockito.when(mockExhibitor.getActivityQueue()).thenReturn(activityQueue);
        Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn("one");
        Mockito.when(mockExhibitor.getRemoteInstanceRequestClient()).thenReturn(mockClient);

        final AtomicLong    modified = new AtomicLong(1);
        ConfigProvider      provider = new ConfigProvider()
        {
            private volatile ConfigCollection      config = new PropertyBasedInstanceConfig(new Properties(), new Properties());

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

        InstanceState       state = new InstanceState(serverList, InstanceStateTypes.SERVING, new RestartSignificantConfig(null));

        ConfigManager       manager = new ConfigManager(mockExhibitor, provider, 10);
        manager.start();
        try
        {
            Properties                      properties = new Properties();
            properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.SERVERS_SPEC, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), serverList.toSpecString());
            PropertyBasedInstanceConfig     config = new PropertyBasedInstanceConfig(properties, DefaultProperties.get(null));
            manager.startRollingConfig(config.getRootConfig());

            for ( String hostname : manager.getRollingConfigState().getRollingHostNames() )
            {
                if ( hostname.equals("two") )
                {
                    continue;
                }

                Assert.assertTrue(manager.isRolling());

                RollingReleaseState     rollingState = new RollingReleaseState(state, manager.getCollection());
                Assert.assertEquals(rollingState.getCurrentRollingHostname(), hostname);

                Assert.assertNull(manager.getRollingConfigAdvanceAttempt());

                Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn(hostname);

                long                lastModified = modified.get();
                manager.checkRollingConfig(state);

                if ( hostname.equals("aaa") )     // the next will be the down instance "two"
                {
                    for ( int i = 1; i < ConfigManager.DEFAULT_MAX_ATTEMPTS; ++i )  // don't check last time as it's cleared on MAX
                    {
                        Assert.assertNotNull(manager.getRollingConfigAdvanceAttempt());
                        Assert.assertEquals(manager.getRollingConfigAdvanceAttempt().getAttemptCount(), i);
                        manager.checkRollingConfig(state);
                    }
                }

                Assert.assertTrue(modified.get() > lastModified);
            }

            Assert.assertFalse(manager.isRolling());
        }
        finally
        {
            Closeables.closeQuietly(manager);
        }
    }

    @Test
    public void testDownLastInstance() throws Exception
    {
        ServerList          serverList = new ServerList("1:one,2:two,3:three");

        RemoteInstanceRequestClient     mockClient = new RemoteInstanceRequestClient()
        {
            @Override
            public <T> T getWebResource(URI remoteUri, MediaType type, Class<T> clazz) throws Exception
            {
                if ( remoteUri.getHost().equals("two") )
                {
                    throw new Exception();
                }
                return clazz.cast("foo");
            }
        };

        ActivityLog         log = new ActivityLog(100);
        ActivityQueue       activityQueue = new ActivityQueue();
        Exhibitor           mockExhibitor = Mockito.mock(Exhibitor.class);
        Mockito.when(mockExhibitor.getLog()).thenReturn(log);
        Mockito.when(mockExhibitor.getActivityQueue()).thenReturn(activityQueue);
        Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn("one");
        Mockito.when(mockExhibitor.getRemoteInstanceRequestClient()).thenReturn(mockClient);

        final AtomicLong    modified = new AtomicLong(1);
        ConfigProvider      provider = new ConfigProvider()
        {
            private volatile ConfigCollection      config = new PropertyBasedInstanceConfig(new Properties(), new Properties());

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

        InstanceState       state = new InstanceState(serverList, InstanceStateTypes.SERVING, new RestartSignificantConfig(null));

        ConfigManager       manager = new ConfigManager(mockExhibitor, provider, 10);
        manager.start();
        try
        {
            Properties                      properties = new Properties();
            properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.SERVERS_SPEC, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), serverList.toSpecString());
            PropertyBasedInstanceConfig     config = new PropertyBasedInstanceConfig(properties, DefaultProperties.get(null));
            manager.startRollingConfig(config.getRootConfig());

            for ( String hostname : manager.getRollingConfigState().getRollingHostNames() )
            {
                if ( hostname.equals("two") )
                {
                    Assert.assertFalse(manager.isRolling());
                    continue;
                }

                Assert.assertTrue(manager.isRolling());

                RollingReleaseState     rollingState = new RollingReleaseState(state, manager.getCollection());
                Assert.assertEquals(rollingState.getCurrentRollingHostname(), hostname);

                Assert.assertNull(manager.getRollingConfigAdvanceAttempt());

                Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn(hostname);

                long                lastModified = modified.get();
                manager.checkRollingConfig(state);

                if ( hostname.equals("three") )     // the next will be the down instance "two"
                {
                    for ( int i = 1; i < ConfigManager.DEFAULT_MAX_ATTEMPTS; ++i )  // don't check last time as it's cleared on MAX
                    {
                        Assert.assertNotNull(manager.getRollingConfigAdvanceAttempt());
                        Assert.assertEquals(manager.getRollingConfigAdvanceAttempt().getAttemptCount(), i);
                        manager.checkRollingConfig(state);
                    }
                }

                Assert.assertTrue(modified.get() > lastModified);
            }

            Assert.assertFalse(manager.isRolling());
        }
        finally
        {
            Closeables.closeQuietly(manager);
        }
    }

    @Test
    public void testChange() throws Exception
    {
        ServerList          serverList = new ServerList("1:one,2:two,3:three");

        RemoteInstanceRequestClient     mockClient = new RemoteInstanceRequestClient()
        {
            @Override
            public <T> T getWebResource(URI remoteUri, MediaType type, Class<T> clazz) throws Exception
            {
                return clazz.cast("foo");
            }
        };

        ActivityLog         log = new ActivityLog(100);
        ActivityQueue       activityQueue = new ActivityQueue();
        Exhibitor           mockExhibitor = Mockito.mock(Exhibitor.class);
        Mockito.when(mockExhibitor.getLog()).thenReturn(log);
        Mockito.when(mockExhibitor.getActivityQueue()).thenReturn(activityQueue);
        Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn("one");
        Mockito.when(mockExhibitor.getRemoteInstanceRequestClient()).thenReturn(mockClient);

        final AtomicLong    modified = new AtomicLong(1);
        ConfigProvider      provider = new ConfigProvider()
        {
            private volatile ConfigCollection      config = new PropertyBasedInstanceConfig(new Properties(), new Properties());

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

        InstanceState       state = new InstanceState(serverList, InstanceStateTypes.SERVING, new RestartSignificantConfig(null));

        ConfigManager       manager = new ConfigManager(mockExhibitor, provider, 10);
        manager.start();
        try
        {
            Properties                      properties = new Properties();
            properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.SERVERS_SPEC, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), serverList.toSpecString());
            PropertyBasedInstanceConfig     config = new PropertyBasedInstanceConfig(properties, DefaultProperties.get(null));
            manager.startRollingConfig(config.getRootConfig());

            for ( String hostname : manager.getRollingConfigState().getRollingHostNames() )
            {
                Assert.assertTrue(manager.isRolling());

                RollingReleaseState     rollingState = new RollingReleaseState(state, manager.getCollection());
                Assert.assertEquals(rollingState.getCurrentRollingHostname(), hostname);
                Assert.assertNull(manager.getRollingConfigAdvanceAttempt());

                Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn(hostname);

                long                lastModified = modified.get();
                manager.checkRollingConfig(state);
                Assert.assertTrue(modified.get() > lastModified);
            }

            Assert.assertFalse(manager.isRolling());
        }
        finally
        {
            Closeables.closeQuietly(manager);
        }
    }
}
