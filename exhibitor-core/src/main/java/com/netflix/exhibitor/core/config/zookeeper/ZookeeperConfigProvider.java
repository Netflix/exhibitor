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
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorWatcher;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.exhibitor.core.config.ConfigCollection;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.PseudoLock;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;
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
    private final Map<String, Long> removedHeartbeatInstances = Maps.newConcurrentMap();
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    private volatile String ephemeralNode;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    private static final String     CONFIG_NODE_NAME = "config";
    private static final String     EPHEMERAL_NODE_NAME = "instance-";
    private static final String     LOCK_PATH = "locks";
    private static final String     CONFIG_PATH = "configs";

    private static final String     SEPARATOR = "|";

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

        PathChildrenCacheListener listener = new PathChildrenCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {
                handleCacheEvent(event);
            }
        };
        cache.getListenable().addListener(listener);
    }

    @Override
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        cache.start();

        makeEphemeralNode();
    }

    @Override
    public void close() throws IOException
    {
        state.set(State.CLOSED);

        Closeables.closeQuietly(cache);
        try
        {
            deleteEphemeralNode();
        }
        catch ( Exception e )
        {
            throw new IOException(e);
        }
    }

    @Override
    public LoadedInstanceConfig loadConfig() throws Exception
    {
        long        lastModified = 0;
        Properties  properties = new Properties();
        ChildData   childData = getConfigNode();
        if ( childData != null )
        {
            lastModified = childData.getStat().getMtime();
            properties.load(new ByteArrayInputStream(childData.getData()));
        }
        PropertyBasedInstanceConfig config = new PropertyBasedInstanceConfig(properties, defaults);
        return new LoadedInstanceConfig(config, lastModified);
    }

    @Override
    public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareLastModified) throws Exception
    {
        PropertyBasedInstanceConfig     propertyBasedInstanceConfig = new PropertyBasedInstanceConfig(config);
        ByteArrayOutputStream           out = new ByteArrayOutputStream();
        propertyBasedInstanceConfig.getProperties().store(out, "Auto-generated by Exhibitor " + hostname);

        byte[]                          bytes = out.toByteArray();
        try
        {
            client.create().forPath(ZKPaths.makePath(configPath, CONFIG_NODE_NAME), bytes);
        }
        catch ( KeeperException.NodeExistsException e )
        {
            client.setData().forPath(ZKPaths.makePath(configPath, CONFIG_NODE_NAME), bytes);
        }

        return new LoadedInstanceConfig(propertyBasedInstanceConfig, System.currentTimeMillis());   // technically, I should get the mTime from the Stat. This should be good enough though.
    }

    @Override
    public void writeInstanceHeartbeat() throws Exception
    {
        // NOP
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean isHeartbeatAliveForInstance(String instanceHostname, int deadInstancePeriodMs) throws Exception
    {
        for ( Map.Entry<String, Long> entry : removedHeartbeatInstances.entrySet() )
        {
            long        elapsedSinceLastHeartbeat = System.currentTimeMillis() - entry.getValue();
            if ( elapsedSinceLastHeartbeat > deadInstancePeriodMs )
            {
                removedHeartbeatInstances.remove(entry.getKey());
            }
        }

        final String  fixedInstanceHostname = fixHostname(instanceHostname);
        if
        (
            // see if it's in the active cache
            Iterables.find
            (
                cache.getCurrentData(),
                new Predicate<ChildData>()
                {
                    @Override
                    public boolean apply(ChildData data)
                    {
                        return ZKPaths.getNodeFromPath(data.getPath()).contains(SEPARATOR + EPHEMERAL_NODE_NAME + fixedInstanceHostname);
                    }
                },
                null
            ) != null
        )
        {
            return true;
        }

        return removedHeartbeatInstances.containsKey(fixedInstanceHostname);    // it's still waiting to be considered dead if it's in removedHeartbeatInstances
    }

    @Override
    public void clearInstanceHeartbeat() throws Exception
    {
        // NOP
    }

    @Override
    public PseudoLock newPseudoLock() throws Exception
    {
        InterProcessMutex   lock = new InterProcessMutex(client, lockPath);
        return new ZookeeperPseudoLock(lock);
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

    private String getHostnameFromEphemeralNode(String nodeName) throws UnsupportedEncodingException
    {
        String[] parts = nodeName.split("\\" + SEPARATOR);
        for ( String p : parts )
        {
            if ( p.startsWith(EPHEMERAL_NODE_NAME) )
            {
                String fixedHostname = p.substring(EPHEMERAL_NODE_NAME.length());
                return unfixHostname(fixedHostname);
            }
        }

        throw new IllegalStateException("Could not find hostname in: " + nodeName);
    }

    private String unfixHostname(String fixedHostname) throws UnsupportedEncodingException
    {
        return URLDecoder.decode(fixedHostname, "UTF-8");
    }

    private synchronized void makeEphemeralNode() throws Exception
    {
        if ( state.get() != State.STARTED )
        {
            return;
        }

        String      fixedHostname = fixHostname(hostname);
        String      nodeName = SEPARATOR + EPHEMERAL_NODE_NAME + fixedHostname + SEPARATOR;
        String      path = ZKPaths.makePath(configPath, nodeName);

        deleteEphemeralNode();
        ephemeralNode = client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);

        CuratorWatcher      watcher = new CuratorWatcher()
        {
            @Override
            public void process(WatchedEvent event) throws Exception
            {
                makeEphemeralNode();
            }
        };
        client.checkExists().usingWatcher(watcher).forPath(ephemeralNode);
    }

    private String fixHostname(String instanceHostname) throws UnsupportedEncodingException
    {
        return URLEncoder.encode(instanceHostname, "UTF-8");
    }

    private void deleteEphemeralNode() throws Exception
    {
        if ( ephemeralNode != null )
        {
            client.delete().guaranteed().forPath(ephemeralNode);
            ephemeralNode = null;
        }
    }

    @VisibleForTesting
    protected void handleCacheEvent(PathChildrenCacheEvent event) throws UnsupportedEncodingException
    {
        if ( isHeartbeatNode(event.getData()) )
        {
            String      instanceHostname = getHostnameFromEphemeralNode(event.getData().getPath());
            if ( (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) )
            {
                removedHeartbeatInstances.put(instanceHostname, System.currentTimeMillis());
            }
            else if ( (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) || (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) )
            {
                removedHeartbeatInstances.remove(instanceHostname);
            }
        }
    }

    private boolean isHeartbeatNode(ChildData data)
    {
        return (data != null) && ZKPaths.getNodeFromPath(data.getPath()).contains(SEPARATOR + EPHEMERAL_NODE_NAME);
    }
}
