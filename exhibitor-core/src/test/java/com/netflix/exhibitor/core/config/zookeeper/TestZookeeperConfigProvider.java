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

package com.netflix.exhibitor.core.config.zookeeper;

import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.StringConfigs;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class TestZookeeperConfigProvider
{
    private TestingCluster cluster;
    private CuratorFramework client;
    private Timing timing;

    @BeforeMethod
    public void setup() throws Exception
    {
        timing = new Timing();

        cluster = new TestingCluster(3);
        cluster.start();
        client = CuratorFrameworkFactory.newClient(cluster.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();
    }

    @AfterMethod
    public void tearDown()
    {
        Closeables.closeQuietly(client);
        Closeables.closeQuietly(cluster);
    }

    @Test
    public void testConcurrentModification() throws Exception
    {
        ZookeeperConfigProvider config1 = new ZookeeperConfigProvider(client, "/foo", new Properties(), "foo");
        ZookeeperConfigProvider config2 = new ZookeeperConfigProvider(client, "/foo", new Properties(), "foo");
        try
        {
            config1.start();
            config2.start();

            final Semaphore     cacheUpdate2 = new Semaphore(0);
            config2.getPathChildrenCache().getListenable().addListener
            (
                new PathChildrenCacheListener()
                {
                    @Override
                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                    {
                        cacheUpdate2.release();
                    }
                }
            );

            Properties              properties = new Properties();
            properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.ZOO_CFG_EXTRA, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), "1,2,3");
            LoadedInstanceConfig    loaded1 = config1.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties()), -1);

            Assert.assertTrue(timing.acquireSemaphore(cacheUpdate2));
            timing.sleepABit();

            LoadedInstanceConfig    loaded2 = config2.loadConfig();
            Assert.assertEquals("1,2,3", loaded2.getConfig().getRootConfig().getString(StringConfigs.ZOO_CFG_EXTRA));

            properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.ZOO_CFG_EXTRA, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), "4,5,6");
            config2.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties()), loaded2.getVersion());

            Assert.assertNull(config1.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties()), loaded1.getVersion()));

            LoadedInstanceConfig    newLoaded1 = config1.loadConfig();
            Assert.assertNotEquals(loaded1.getVersion(), newLoaded1.getVersion());
        }
        finally
        {
            Closeables.closeQuietly(config2);
            Closeables.closeQuietly(config1);
        }
    }

    @Test
    public void testBasic() throws Exception
    {
        ZookeeperConfigProvider config = new ZookeeperConfigProvider(client, "/foo", new Properties(), "foo");
        try
        {
            config.start();

            config.loadConfig();    // make sure there's no exception

            Properties      properties = new Properties();
            properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.ZOO_CFG_EXTRA, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), "1,2,3");
            config.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties()), 0);

            timing.sleepABit();

            LoadedInstanceConfig instanceConfig = config.loadConfig();
            Assert.assertEquals("1,2,3", instanceConfig.getConfig().getRootConfig().getString(StringConfigs.ZOO_CFG_EXTRA));
        }
        finally
        {
            Closeables.closeQuietly(config);
        }
    }
}
