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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.config.ConfigCollection;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.PseudoLock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class ZookeeperConfigProvider implements ConfigProvider
{
    private final PathChildrenCache cache;
    private final CuratorFramework client;
    private final Properties defaults;
    private final String hostname;
    private final String configPath;
    private final String lockPath;
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    private static final String     CONFIG_NODE_NAME = "config";
    private static final String     LOCK_PATH = "locks";
    private static final String     CONFIG_PATH = "configs";

    /**
     * @param client curator instance for connecting to the config ensemble
     * @param baseZPath the base path for config nodes
     * @param defaults default properties
     * @param hostname this JVM's hostname
     * @throws Exception errors
     */
    public ZookeeperConfigProvider(CuratorFramework client, String baseZPath, Properties defaults, String hostname) throws Exception
    {
        this.client = client;
        this.defaults = defaults;
        this.hostname = hostname;
        configPath = ZKPaths.makePath(baseZPath, CONFIG_PATH);
        lockPath = ZKPaths.makePath(baseZPath, LOCK_PATH);
        cache = new PathChildrenCache(client, configPath, true);
    }

    @Override
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        cache.start();
    }

    @Override
    public void close() throws IOException
    {
        state.set(State.CLOSED);

        Closeables.closeQuietly(cache);
    }

    @Override
    public LoadedInstanceConfig loadConfig() throws Exception
    {
        int         version = -1;
        Properties  properties = new Properties();
        ChildData   childData = getConfigNode();
        if ( childData != null )
        {
            version = childData.getStat().getVersion();
            properties.load(new ByteArrayInputStream(childData.getData()));
        }
        PropertyBasedInstanceConfig config = new PropertyBasedInstanceConfig(properties, defaults);
        return new LoadedInstanceConfig(config, version);
    }

    @Override
    public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareVersion) throws Exception
    {
        PropertyBasedInstanceConfig     propertyBasedInstanceConfig = new PropertyBasedInstanceConfig(config);
        ByteArrayOutputStream           out = new ByteArrayOutputStream();
        propertyBasedInstanceConfig.getProperties().store(out, "Auto-generated by Exhibitor " + hostname);

        byte[]          bytes = out.toByteArray();
        int             newVersion;
        try
        {
            Stat        stat = client.setData().withVersion((int)compareVersion).forPath(ZKPaths.makePath(configPath, CONFIG_NODE_NAME), bytes);
            newVersion = stat.getVersion();
        }
        catch ( KeeperException.BadVersionException e )
        {
            return null;    // another process got in first
        }
        catch ( KeeperException.NoNodeException e )
        {
            try
            {
                client.create().creatingParentsIfNeeded().forPath(ZKPaths.makePath(configPath, CONFIG_NODE_NAME), bytes);
                newVersion = 0;
            }
            catch ( KeeperException.NodeExistsException e1 )
            {
                return null;    // by implication, another process created the node first
            }
        }

        return new LoadedInstanceConfig(propertyBasedInstanceConfig, newVersion);
    }

    @Override
    public PseudoLock newPseudoLock() throws Exception
    {
        InterProcessMutex   lock = new InterProcessMutex(client, lockPath);
        return new ZookeeperPseudoLock(lock);
    }

    @VisibleForTesting
    PathChildrenCache getPathChildrenCache()
    {
        return cache;
    }

    private ChildData getConfigNode()
    {
        return Iterables.find
            (
                cache.getCurrentData(),
                new Predicate<ChildData>()
                {
                    @Override
                    public boolean apply(ChildData data)
                    {
                        return ZKPaths.getNodeFromPath(data.getPath()).equals(CONFIG_NODE_NAME);
                    }
                },
                null
            );
    }
}
