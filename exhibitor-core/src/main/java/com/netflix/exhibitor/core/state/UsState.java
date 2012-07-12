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

import com.google.common.collect.Iterables;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.StringConfigs;

public class UsState
{
    private final InstanceConfig config;
    private final ServerList serverList;
    private final ServerSpec us;

    public UsState(Exhibitor exhibitor)
    {
        config = exhibitor.getConfigManager().getConfig();
        serverList = new ServerList(config.getString(StringConfigs.SERVERS_SPEC));
        us = Iterables.find(serverList.getSpecs(), ServerList.isUs(exhibitor.getThisJVMHostname()), null);
    }

    public InstanceConfig getConfig()
    {
        return config;
    }

    public ServerList getServerList()
    {
        return serverList;
    }

    public ServerSpec getUs()
    {
        return us;
    }
}
