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

package com.netflix.exhibitor.core.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.PseudoLock;
import com.netflix.exhibitor.core.config.StringConfigs;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AutomaticInstanceManagement implements Activity
{
    private final Exhibitor exhibitor;
    private final int minInstances;

    private static final int DEFAULT_MIN_INSTANCES = 3;

    public AutomaticInstanceManagement(Exhibitor exhibitor)
    {
        this(exhibitor, DEFAULT_MIN_INSTANCES);
    }

    public AutomaticInstanceManagement(Exhibitor exhibitor, int minInstances)
    {
        this.exhibitor = exhibitor;
        this.minInstances = minInstances;
    }

    @Override
    public void completed(boolean wasSuccessful)
    {
    }

    @Override
    public Boolean call() throws Exception
    {
        if ( exhibitor.getConfigManager().getConfig().getInt(IntConfigs.AUTO_MANAGE_INSTANCES) != 0 )
        {
            PseudoLock  lock = exhibitor.getConfigManager().newConfigBasedLock();
            try
            {
                if ( lock.lock(Exhibitor.AUTO_INSTANCE_MANAGEMENT_PERIOD_MS / 2, TimeUnit.MILLISECONDS) )
                {
                    if ( !exhibitor.getConfigManager().isRolling() )
                    {
                        doWork();
                    }
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        return true;
    }

    @VisibleForTesting
    protected void doWork() throws Exception
    {
        UsState usState = new UsState(exhibitor);

        exhibitor.getConfigManager().writeHeartbeat();
        if ( exhibitor.getMonitorRunningInstance().getCurrentInstanceState() != InstanceStateTypes.LATENT )
        {
            if ( usState.getUs() == null )
            {
                addUsIn(usState);
            }
        }

        checkForStaleInstances(usState);
    }

    private void checkForStaleInstances(UsState usState) throws Exception
    {
        List<ServerSpec>            serverSpecList = usState.getServerList().getSpecs();
        if ( serverSpecList.size() <= minInstances )
        {
            return;
        }

        List<ServerSpec>            newSpecList = Lists.newArrayList();
        List<String>                removals = Lists.newArrayList();
        for ( ServerSpec spec : serverSpecList )
        {
            if ( (usState.getUs() != null) && usState.getUs().equals(spec) )
            {
                newSpecList.add(spec);
            }
            else
            {
                long        lastHeartbeatForInstance = exhibitor.getConfigManager().getLastHeartbeatForInstance(spec.getHostname());
                long        elapsedSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatForInstance;
                if ( (lastHeartbeatForInstance <= 0) || (elapsedSinceLastHeartbeat <= exhibitor.getConfigManager().getConfig().getInt(IntConfigs.DEAD_INSTANCE_PERIOD_MS)) )
                {
                    newSpecList.add(spec);
                }
                else
                {
                    removals.add(spec.getHostname());
                }
            }
        }

        if ( removals.size() > 0 )
        {
            List<String>    transformed = Lists.transform
            (
                newSpecList,
                new Function<ServerSpec, String>()
                {
                    @Override
                    public String apply(ServerSpec spec)
                    {
                        return spec.toSpecString();
                    }
                }
            );
            String          newSpec = Joiner.on(',').join(transformed);
            String          reason = Joiner.on(", ").join(removals);
            adjustConfig(exhibitor.getConfigManager().getConfig(), newSpec, "Removing stale instance(s) from servers list: " + reason);
        }
    }

    private void addUsIn(UsState usState) throws Exception
    {
        int         maxServerId = 0;
        for ( ServerSpec spec : usState.getServerList().getSpecs() )
        {
            if ( spec.getServerId() > maxServerId )
            {
                maxServerId = spec.getServerId();
            }
        }

        int                     observerThreshold = exhibitor.getConfigManager().getConfig().getInt(IntConfigs.OBSERVER_THRESHOLD);
        int                     newUsIndex = usState.getServerList().getSpecs().size() + 1;
        ServerType              serverType = (newUsIndex >= observerThreshold) ? ServerType.OBSERVER : ServerType.STANDARD;

        InstanceConfig          currentConfig = exhibitor.getConfigManager().getConfig();
        String                  spec = currentConfig.getString(StringConfigs.SERVERS_SPEC);
        String                  thisValue = new ServerSpec(exhibitor.getThisJVMHostname(), maxServerId + 1, serverType).toSpecString();
        final String            newSpec = Joiner.on(',').skipNulls().join((spec.length() > 0) ? spec : null, thisValue);
        adjustConfig(currentConfig, newSpec, "Adding this instance to server list due to automatic instance management");
    }

    private void adjustConfig(final InstanceConfig currentConfig, final String newSpec, String reason) throws Exception
    {
        InstanceConfig          newConfig = new InstanceConfig()
        {
            @Override
            public String getString(StringConfigs config)
            {
                if ( config == StringConfigs.SERVERS_SPEC )
                {
                    return newSpec;
                }
                return currentConfig.getString(config);
            }

            @Override
            public int getInt(IntConfigs config)
            {
                return currentConfig.getInt(config);
            }
        };
        if ( exhibitor.getConfigManager().startRollingConfig(newConfig) ) // if this fails due to an old config it's fine - it will just try again next time
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, reason);
        }
    }
}
