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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.entities.ServerStatus;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import com.netflix.exhibitor.core.state.ServerList;
import com.netflix.exhibitor.core.state.ServerSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class ClusterState
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AtomicReference<List<ServerStatus>> statuses = new AtomicReference<List<ServerStatus>>();
    private final AtomicReference<ServerList> configuredServerList = new AtomicReference<ServerList>();
    private final AtomicLong updateTimeMs = new AtomicLong();

    ClusterState()
    {
        clear();
    }

    void update(ServerList configuredServerList, List<ServerStatus> newStatuses)
    {
        this.configuredServerList.set(configuredServerList);

        List<ServerStatus> currentStatuses = statuses.get();
        if ( !currentStatuses.equals(newStatuses) )
        {
            statuses.set(ImmutableList.copyOf(newStatuses));
            updateTimeMs.set(System.currentTimeMillis());
        }
    }

    boolean isStable(long stabilityPeriodMs)
    {
        long elapsed = System.currentTimeMillis() - updateTimeMs.get();
        return elapsed >= stabilityPeriodMs;
    }

    boolean isInQuorum()
    {
        List<ServerStatus> currentStatuses = statuses.get();
        return (currentStatuses.size() == 0) || (getLeaderHostname() != null);
    }

    ServerList getConfiguredServerList()
    {
        return configuredServerList.get();
    }

    Map<ServerSpec, InstanceStateTypes> buildStatusMap()
    {
        ServerList serverList = configuredServerList.get();

        ImmutableMap.Builder<ServerSpec, InstanceStateTypes> builder = ImmutableMap.builder();
        List<ServerStatus> currentStatuses = statuses.get();
        for ( ServerStatus status : currentStatuses )
        {
            ServerSpec spec = serverList.getSpec(status.getHostname());
            if ( spec != null )
            {
                builder.put(spec, status.getInstanceStateType());
            }
            else
            {
                log.error("No configured spec found for hostname: " + status.getHostname());
            }
        }
        return builder.build();
    }

    List<ServerStatus> getLiveInstances()
    {
        List<ServerStatus> live = Lists.newArrayList();
        List<ServerStatus> currentStatuses = statuses.get();
        for ( ServerStatus status : currentStatuses )
        {
            InstanceStateTypes type = InstanceStateTypes.fromCode(status.getCode());
            if ( type != InstanceStateTypes.DOWN )
            {
                live.add(status);
            }
        }

        return live;
    }

    boolean hasDeadInstances()
    {
        List<ServerStatus> currentStatuses = statuses.get();
        for ( ServerStatus status : currentStatuses )
        {
            InstanceStateTypes type = InstanceStateTypes.fromCode(status.getCode());
            if ( type == InstanceStateTypes.DOWN )
            {
                return true;
            }
        }
        return false;
    }

    String getLeaderHostname()
    {
        List<ServerStatus> currentStatuses = statuses.get();
        for ( ServerStatus status : currentStatuses )
        {
            if ( status.getIsLeader() )
            {
                return status.getHostname();
            }
        }
        return null;
    }

    void clear()
    {
        statuses.set(Lists.<ServerStatus>newArrayList());
        updateTimeMs.set(System.currentTimeMillis());
        configuredServerList.set(new ServerList(Lists.<ServerSpec>newArrayList()));
    }
}
