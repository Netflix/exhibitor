/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.exhibitor.core.state;

public class InstanceState
{
    private final InstanceStateTypes        state;
    private final ServerList                serverList;
    private final RestartSignificantConfig  currentConfig;

    private volatile long                      timestampMs;

    public InstanceState()
    {
        state = InstanceStateTypes.LATENT;
        serverList = new ServerList("");
        currentConfig = null;
    }

    public InstanceState(ServerList serverList, InstanceStateTypes state, RestartSignificantConfig currentConfig)
    {
        this.serverList = serverList;
        this.state = state;
        this.currentConfig = currentConfig;

        updateTimestampMs();
    }

    public ServerList getServerList()
    {
        return serverList;
    }

    public InstanceStateTypes getState()
    {
        return state;
    }

    public RestartSignificantConfig getCurrentConfig()
    {
        return currentConfig;
    }

    public long getTimestampMs()
    {
        return timestampMs;
    }

    public void updateTimestampMs()
    {
        timestampMs = System.currentTimeMillis();
    }

    @SuppressWarnings("RedundantIfStatement")
    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        InstanceState that = (InstanceState)o;

        if ( !currentConfig.equals(that.currentConfig) )
        {
            return false;
        }
        if ( !serverList.equals(that.serverList) )
        {
            return false;
        }
        if ( state != that.state )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = state.hashCode();
        result = 31 * result + serverList.hashCode();
        result = 31 * result + currentConfig.hashCode();
        return result;
    }
}
