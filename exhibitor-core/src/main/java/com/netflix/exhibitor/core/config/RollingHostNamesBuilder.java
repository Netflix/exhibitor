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

package com.netflix.exhibitor.core.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.state.ServerList;
import java.util.List;
import java.util.Set;

class RollingHostNamesBuilder
{
    private final List<String>      rollingHostNames;

    RollingHostNamesBuilder(InstanceConfig rootConfig, InstanceConfig rollingConfig, ActivityLog log)
    {
        ServerList  rootServers = new ServerList(rootConfig.getString(StringConfigs.SERVERS_SPEC));
        ServerList  rollingServers = new ServerList(rollingConfig.getString(StringConfigs.SERVERS_SPEC));

        Set<String>     newServers = Sets.difference(Sets.newTreeSet(rollingServers.getHostnames()), Sets.newTreeSet(rootServers.getHostnames()));
        Set<String>     unchangedServers = Sets.intersection(Sets.newTreeSet(rollingServers.getHostnames()), Sets.newTreeSet(rootServers.getHostnames()));

        if ( newServers.size() > 1 )
        {
            log.add(ActivityLog.Type.INFO, "Warning - adding more than 1 new server can cause issues. The ensemble may not achieve quorum.");
        }

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.addAll(newServers); // new servers need to be started first as the others will try to communicate with them. You may have issues if there is more than 1 new server
        builder.addAll(unchangedServers);   // the servers that are staying in the cluster can be restarted next.
        rollingHostNames = builder.build(); // servers coming out of the cluster can be restarted all at once at the end
    }

    List<String> getRollingHostNames()
    {
        return rollingHostNames;
    }
}
