package com.netflix.exhibitor.core.state;

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.config.ConfigManager;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.StringConfigs;
import com.netflix.exhibitor.core.controlpanel.ControlPanelTypes;
import com.netflix.exhibitor.core.controlpanel.ControlPanelValues;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.prefs.Preferences;

public class TestMonitorRunningInstance
{
    @Test
    public void testTempDownInstance() throws Exception
    {
        final AtomicInteger         checkMs = new AtomicInteger(10000);
        InstanceConfig              config = new InstanceConfig()
        {
            @Override
            public String getString(StringConfigs config)
            {
                switch ( config )
                {
                    case SERVERS_SPEC:
                    {
                        return "1:foo,2:bar";
                    }

                    case ZOOKEEPER_DATA_DIRECTORY:
                    case ZOOKEEPER_INSTALL_DIRECTORY:
                    {
                        return "/";
                    }
                }
                return null;
            }

            @Override
            public int getInt(IntConfigs config)
            {
                switch ( config )
                {
                    case CHECK_MS:
                    {
                        return checkMs.get();
                    }
                }
                return 0;
            }
        };

        Preferences                 preferences = Mockito.mock(Preferences.class);
        ControlPanelValues          controlPanelValues = new ControlPanelValues(preferences)
        {
            @Override
            public boolean isSet(ControlPanelTypes type) throws Exception
            {
                return true;
            }
        };

        ConfigManager               configManager = Mockito.mock(ConfigManager.class);
        Mockito.when(configManager.getConfig()).thenReturn(config);

        Exhibitor                   mockExhibitor = Mockito.mock(Exhibitor.class, Mockito.RETURNS_MOCKS);
        Mockito.when(mockExhibitor.getConfigManager()).thenReturn(configManager);
        Mockito.when(mockExhibitor.getThisJVMHostname()).thenReturn("foo");
        Mockito.when(mockExhibitor.getControlPanelValues()).thenReturn(controlPanelValues);

        final Semaphore             semaphore = new Semaphore(0);
        MonitorRunningInstance      monitor = new MonitorRunningInstance(mockExhibitor)
        {
            @Override
            protected void restartZooKeeper(InstanceState currentInstanceState) throws Exception
            {
                semaphore.release();
            }
        };
        monitor.doWork();

        Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));

        monitor.doWork();
        Assert.assertFalse(semaphore.tryAcquire(3, TimeUnit.SECONDS));  // no instance state change, should not try restart

        checkMs.set(1);
        monitor.doWork();   // should do restart now as 10 times checkMs has elapsed
        Assert.assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS));
    }
}
