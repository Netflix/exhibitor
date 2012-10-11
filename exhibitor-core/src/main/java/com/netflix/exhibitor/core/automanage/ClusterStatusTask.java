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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.entities.ServerStatus;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import com.netflix.exhibitor.core.state.ServerList;
import com.netflix.exhibitor.core.state.ServerSpec;
import jsr166y.RecursiveTask;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;

public class ClusterStatusTask extends RecursiveTask<List<ServerStatus>>
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Exhibitor exhibitor;
    private final List<ServerSpec> specs;
    private final ServerSpec us;

    public ClusterStatusTask(Exhibitor exhibitor, List<ServerSpec> specs)
    {
        this.exhibitor = exhibitor;
        this.specs = ImmutableList.copyOf(specs);
        us = Iterables.find(specs, ServerList.isUs(exhibitor.getThisJVMHostname()), null);
    }

    @Override
    protected List<ServerStatus> compute()
    {
        List<ServerStatus>      statuses = Lists.newArrayList();

        int size = specs.size();
        switch ( size )
        {
            case 0:
            {
                break;  // nothing to do
            }

            case 1:
            {
                statuses.add(getStatus(specs.get(0)));
                break;
            }

            default:
            {
                List<ClusterStatusTask> tasks = Lists.newArrayList();
                for ( List<ServerSpec> subList : Lists.partition(specs, size / 2) )
                {
                    ClusterStatusTask task = new ClusterStatusTask(exhibitor, subList);
                    task.fork();
                    tasks.add(task);
                }

                for ( ClusterStatusTask task : tasks )
                {
                    statuses.addAll(task.join());
                }
                break;
            }
        }
        return statuses;
    }

    private ServerStatus getStatus(ServerSpec spec)
    {
        if ( spec.equals(us) )
        {
            InstanceStateTypes state = exhibitor.getMonitorRunningInstance().getCurrentInstanceState();
            return new ServerStatus(spec.getHostname(), state.getCode(), state.getDescription(), exhibitor.getMonitorRunningInstance().isCurrentlyLeader());
        }

        try
        {
            RemoteInstanceRequest           request = new RemoteInstanceRequest(exhibitor, spec.getHostname());
            RemoteInstanceRequest.Result    result = request.makeRequest(exhibitor.getRemoteInstanceRequestClient(), "state");

            ObjectMapper                    mapper = new ObjectMapper();
            JsonNode                        value = mapper.readTree(mapper.getJsonFactory().createJsonParser(result.remoteResponse));
            if ( value.size() == 0 )
            {
                return new ServerStatus(spec.getHostname(), InstanceStateTypes.DOWN.getCode(), InstanceStateTypes.DOWN.getDescription(), false);
            }

            int                             code = value.get("state").asInt();
            String                          description = value.get("description").asText();
            return new ServerStatus(spec.getHostname(), code, description, value.get("isLeader").asBoolean());
        }
        catch ( IOException e )
        {
            log.error("Getting remote server status", e);
            throw new RuntimeException(e);
        }
    }
}
