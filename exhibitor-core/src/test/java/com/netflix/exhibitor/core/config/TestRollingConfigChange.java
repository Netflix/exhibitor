package com.netflix.exhibitor.core.config;

import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.ActivityQueue;
import com.netflix.exhibitor.core.state.InstanceState;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import com.netflix.exhibitor.core.state.RestartSignificantConfig;
import com.netflix.exhibitor.core.state.ServerList;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class TestRollingConfigChange
{
    @Test
    public void testChange() throws Exception
    {
        ServerList          serverList = new ServerList("1:one,2:two,3:three");

        ActivityLog         log = new ActivityLog(100);
        ActivityQueue       activityQueue = new ActivityQueue();
        Exhibitor           mockExhibitor = Mockito.mock(Exhibitor.class);
        Mockito.when(mockExhibitor.getLog()).thenReturn(log);
        Mockito.when(mockExhibitor.getActivityQueue()).thenReturn(activityQueue);
        Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn("one");

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
                // TODO
            }

            @Override
            public long getLastHeartbeatForInstance(String instanceHostname) throws Exception
            {
                // TODO
                return 0;
            }

            @Override
            public PseudoLock newPseudoLock() throws Exception
            {
                return null;    // TODO
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
