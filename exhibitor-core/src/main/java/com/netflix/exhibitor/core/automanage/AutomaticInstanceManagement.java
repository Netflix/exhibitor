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
import java.util.List;
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

        ServerList serverList = new ServerList(exhibitor.getConfigManager().getConfig().getString(StringConfigs.SERVERS_SPEC));
        List<ServerStatus> statuses = getStatuses(serverList);
        clusterState.update(serverList, statuses);

        EnsembleBuilder ensembleBuilder = (exhibitor.getConfigManager().getConfig().getInt(IntConfigs.AUTO_MANAGE_INSTANCES_FIXED_ENSEMBLE_SIZE) > 0) ? new FixedEnsembleBuilder(exhibitor, clusterState) : new FlexibleEnsembleBuilder(exhibitor, clusterState);

        if ( !ensembleBuilder.newEnsembleNeeded() )
        {
            return true;
        }

        if ( !applyAllAtOnce() && !clusterState.isInQuorum() )
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Ensemble is not currently in quorum. Automatic Instance Management will wait for quorum. NOTE: if \"Apply All At Once\" is set to \"yes\", this quorum check is not needed.");
            return true;
        }

        int settlingPeriodMs = exhibitor.getConfigManager().getConfig().getInt(IntConfigs.AUTO_MANAGE_INSTANCES_SETTLING_PERIOD_MS);
        if ( !clusterState.isStable(settlingPeriodMs) )
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Ensemble state is not yet stable. Automatic Instance Management will wait for stability.");
            return true;
        }

        PseudoLock lock = exhibitor.getConfigManager().newConfigBasedLock();
        try
        {
            if ( lock.lock(exhibitor.getLog(), Exhibitor.AUTO_INSTANCE_MANAGEMENT_PERIOD_MS / 2, TimeUnit.MILLISECONDS) )
            {
                ServerList potentialServerList = ensembleBuilder.createPotentialServerList();
                if ( !potentialServerList.equals(serverList) )  // otherwise, no change
                {
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
        final InstanceConfig currentConfig = exhibitor.getConfigManager().getConfig();
        InstanceConfig newConfig = new InstanceConfig()
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

        boolean success;
        if ( applyAllAtOnce() )
        {
            success = exhibitor.getConfigManager().updateConfig(newConfig);
        }
        else
        {
            success = exhibitor.getConfigManager().startRollingConfig(newConfig, leaderHostname);
        }
        if ( success )
        {
            clusterState.clear();
        }
        else
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Could not initiate Automatic Instance Management config change. Another process is already making a config change.");
        }
    }

    private boolean applyAllAtOnce()
    {
        return exhibitor.getConfigManager().getConfig().getInt(IntConfigs.AUTO_MANAGE_INSTANCES_APPLY_ALL_AT_ONCE) != 0;
    }

    private List<ServerStatus> getStatuses(ServerList serverList)
    {
        exhibitor.getLog().add(ActivityLog.Type.DEBUG, "Automatic Instance Management querying for instance statuses...");

        ClusterStatusTask task = new ClusterStatusTask(exhibitor, serverList.getSpecs());
        List<ServerStatus> statuses = exhibitor.getForkJoinPool().invoke(task);

        exhibitor.getLog().add(ActivityLog.Type.DEBUG, "Instance statuses query done.");

        return statuses;
    }
}
