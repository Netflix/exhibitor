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

package com.netflix.exhibitor.core.automanage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.PseudoLock;
import com.netflix.exhibitor.core.config.StringConfigs;
import com.netflix.exhibitor.core.entities.ServerStatus;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import com.netflix.exhibitor.core.state.ServerList;
import com.netflix.exhibitor.core.state.ServerSpec;
import com.netflix.exhibitor.core.state.ServerType;
import com.netflix.exhibitor.core.state.UsState;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class AutomaticInstanceManagement implements Activity
{
    private final Exhibitor exhibitor;
    private final ClusterState clusterState = new ClusterState();

    public AutomaticInstanceManagement(Exhibitor exhibitor)
    {
        this.exhibitor = exhibitor;
    }

    @Override
    public void completed(boolean wasSuccessful)
    {
    }

    @Override
    public Boolean call() throws Exception
    {
        if ( exhibitor.getConfigManager().getConfig().getInt(IntConfigs.AUTO_MANAGE_INSTANCES) == 0 )
        {
            return true;    // auto manage is turned off
        }

        if ( exhibitor.getConfigManager().isRolling() )
        {
            return true;
        }

        if ( exhibitor.getMonitorRunningInstance().getCurrentInstanceState() == InstanceStateTypes.LATENT )
        {
            return true;    // this instance hasn't warmed up yet
        }

        ServerList          serverList = new ServerList(exhibitor.getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC));
        List<ServerStatus>  statuses = getStatuses(serverList);

        clusterState.update(statuses);

        UsState             usState = new UsState(exhibitor);
        if ( (usState.getUs() != null) && !clusterState.hasDeadInstances() )
        {
            return true;
        }

        if ( !clusterState.isInQuorum() )
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Ensemble is not currently quorum. Automatic Instance Management will wait for quorum.");
            return true;
        }

        if ( !clusterState.isStable(exhibitor.getConfigManager().getConfig().getInt(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS)) )
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Ensemble state is not yet stable. Automatic Instance Management will wait for stability.");
            return true;
        }

        PseudoLock lock = exhibitor.getConfigManager().newConfigBasedLock();
        try
        {
            if ( lock.lock(exhibitor.getLog(), Exhibitor.AUTO_INSTANCE_MANAGEMENT_PERIOD_MS / 2, TimeUnit.MILLISECONDS) )
            {
                ServerList      potentialServerList = createPotentialServerList(serverList, clusterState.getLiveInstances(), usState.getUs() == null);
                if ( potentialServerList.getSpecs().size() == 0 )
                {
                    exhibitor.getLog().add(ActivityLog.Type.INFO, "Automatic Instance Management skipped because new potential server list is empty");
                }
                else
                {
                    exhibitor.getLog().add(ActivityLog.Type.INFO, "Automatic Instance Management will change the server list: " + serverList + " ==> " + potentialServerList);
                    adjustConfig(potentialServerList.toSpecString(), clusterState.getLeaderHostname());
                }
            }
        }
        finally
        {
            lock.unlock();
        }

        return true;
    }

    @VisibleForTesting
    void adjustConfig(final String newSpec, String leaderHostname) throws Exception
    {
        final InstanceConfig    currentConfig = exhibitor.getConfigManager().getConfig();
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
        if ( exhibitor.getConfigManager().startRollingConfig(newConfig, leaderHostname) )
        {
            clusterState.clear();
        }
        else
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Could not initiate Automatic Instance Management config change. Another process is already making a config change.");
        }
    }

    private ServerList  createPotentialServerList(ServerList existingList, List<ServerStatus> statuses, boolean addUsIn)
    {
        List<ServerSpec>        newList = Lists.newArrayList();
        int                     existingMaxId = 0;
        for ( ServerSpec spec : existingList.getSpecs() )
        {
            if ( spec.getServerId() > existingMaxId )
            {
                existingMaxId = spec.getServerId();
            }
        }

        Set<String>     addedHostnames = Sets.newHashSet();
        for ( ServerStatus status : statuses )
        {
            ServerSpec spec = existingList.getSpec(status.getHostname());
            if ( spec == null )
            {
                spec = new ServerSpec(status.getHostname(), ++existingMaxId, ServerType.STANDARD);
                addedHostnames.add(spec.getHostname());
            }
            newList.add(spec);
        }

        if ( addUsIn )
        {
            ServerSpec      spec = new ServerSpec(exhibitor.getThisJVMHostname(), ++existingMaxId, ServerType.STANDARD);
            addedHostnames.add(spec.getHostname());
            newList.add(spec);
        }

        int                 standardTypeCount = 0;
        for ( ServerSpec spec : newList )
        {
            if ( spec.getServerType() == ServerType.STANDARD )
            {
                ++standardTypeCount;
            }
        }

        int         observerThreshold = exhibitor.getConfigManager().getConfig().getInt(IntConfigs.OBSERVER_THRESHOLD);
        for ( int i = 0; (standardTypeCount >= observerThreshold) && (i < newList.size()); ++i )
        {
            ServerSpec      spec = newList.get(i);
            if ( addedHostnames.contains(spec.getHostname()) )  // i.e. don't change existing instances to observer
            {
                newList.set(i, new ServerSpec(spec.getHostname(), spec.getServerId(), ServerType.OBSERVER));
                --standardTypeCount;
            }
        }

        return new ServerList(newList);
    }

    private List<ServerStatus> getStatuses(ServerList serverList)
    {
        exhibitor.getLog().add(ActivityLog.Type.DEBUG, "Automatic Instance Management querying for instance statuses...");

        ClusterStatusTask   task = new ClusterStatusTask(exhibitor, serverList.getSpecs());
        List<ServerStatus>  statuses = exhibitor.getForkJoinPool().invoke(task);

        exhibitor.getLog().add(ActivityLog.Type.DEBUG, "Instance statuses query done.");

        return statuses;
    }
}
