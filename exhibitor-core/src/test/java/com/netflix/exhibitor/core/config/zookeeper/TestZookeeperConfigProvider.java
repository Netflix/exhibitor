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
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.test.TestingCluster;
import com.netflix.curator.test.Timing;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.StringConfigs;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

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

    @Test
    public void testAlive() throws Exception
    {
        ZookeeperConfigProvider configA = new ZookeeperConfigProvider(client, "/foo", new Properties(), "a");
        ZookeeperConfigProvider configB = new ZookeeperConfigProvider(client, "/foo", new Properties(), "b");
        ZookeeperConfigProvider configC = new ZookeeperConfigProvider(client, "/foo", new Properties(), "c");
        try
        {
            configA.start();
            configB.start();
            configC.start();

            timing.sleepABit();

            Assert.assertTrue(configA.isHeartbeatAliveForInstance("a", 10000));
            Assert.assertTrue(configA.isHeartbeatAliveForInstance("b", 10000));
            Assert.assertTrue(configA.isHeartbeatAliveForInstance("c", 10000));

            Assert.assertTrue(configB.isHeartbeatAliveForInstance("a", 10000));
            Assert.assertTrue(configB.isHeartbeatAliveForInstance("b", 10000));
            Assert.assertTrue(configB.isHeartbeatAliveForInstance("c", 10000));

            Assert.assertTrue(configC.isHeartbeatAliveForInstance("a", 10000));
            Assert.assertTrue(configC.isHeartbeatAliveForInstance("b", 10000));
            Assert.assertTrue(configC.isHeartbeatAliveForInstance("c", 10000));

            Assert.assertFalse(configA.isHeartbeatAliveForInstance("z", 10000));
            Assert.assertFalse(configB.isHeartbeatAliveForInstance("z", 10000));
            Assert.assertFalse(configC.isHeartbeatAliveForInstance("z", 10000));
        }
        finally
        {
            Closeables.closeQuietly(configC);
            Closeables.closeQuietly(configB);
            Closeables.closeQuietly(configA);
        }
    }

    @Test
    public void testInstanceDies() throws Exception
    {
        final CountDownLatch    aAddLatch = new CountDownLatch(3);
        final CountDownLatch    bAddLatch = new CountDownLatch(3);
        final CountDownLatch    cAddLatch = new CountDownLatch(3);

        final CountDownLatch    bRemovalLatch = new CountDownLatch(1);
        final CountDownLatch    cRemovalLatch = new CountDownLatch(1);

        ZookeeperConfigProvider configA = new ZookeeperConfigProvider(client, "/foo", new Properties(), "a")
        {
            @Override
            protected void handleCacheEvent(PathChildrenCacheEvent event) throws UnsupportedEncodingException
            {
                super.handleCacheEvent(event);
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    aAddLatch.countDown();
                }
            }
        };
        ZookeeperConfigProvider configB = new ZookeeperConfigProvider(client, "/foo", new Properties(), "b")
        {
            @Override
            protected void handleCacheEvent(PathChildrenCacheEvent event) throws UnsupportedEncodingException
            {
                super.handleCacheEvent(event);
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    bAddLatch.countDown();
                }
                else if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED )
                {
                    bRemovalLatch.countDown();
                }
            }
        };
        ZookeeperConfigProvider configC = new ZookeeperConfigProvider(client, "/foo", new Properties(), "c")
        {
            @Override
            protected void handleCacheEvent(PathChildrenCacheEvent event) throws UnsupportedEncodingException
            {
                super.handleCacheEvent(event);
                if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED )
                {
                    cAddLatch.countDown();
                }
                else if ( event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED )
                {
                    cRemovalLatch.countDown();
                }
            }
        };
        try
        {
            configA.start();
            configB.start();
            configC.start();

            Assert.assertTrue(timing.awaitLatch(aAddLatch));
            Assert.assertTrue(timing.awaitLatch(bAddLatch));
            Assert.assertTrue(timing.awaitLatch(cAddLatch));

            configA.close();    // simulate dying
            configA = null;

            Assert.assertTrue(timing.awaitLatch(bRemovalLatch));
            Assert.assertTrue(timing.awaitLatch(cRemovalLatch));

            Assert.assertTrue(configB.isHeartbeatAliveForInstance("a", Integer.MAX_VALUE));
            Assert.assertTrue(configC.isHeartbeatAliveForInstance("a", Integer.MAX_VALUE));

            Assert.assertFalse(configB.isHeartbeatAliveForInstance("a", -1));
            Assert.assertFalse(configC.isHeartbeatAliveForInstance("a", -1));
        }
        finally
        {
            Closeables.closeQuietly(configC);
            Closeables.closeQuietly(configB);
            Closeables.closeQuietly(configA);
        }
    }
}
