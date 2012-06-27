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
