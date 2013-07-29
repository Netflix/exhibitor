package com.netflix.exhibitor.core.automanage;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.entities.ServerStatus;
import com.netflix.exhibitor.core.state.ServerList;
import com.netflix.exhibitor.core.state.ServerSpec;
import com.netflix.exhibitor.core.state.ServerType;
import com.netflix.exhibitor.core.state.UsState;
import java.util.List;
import java.util.Set;

class FlexibleEnsembleBuilder implements EnsembleBuilder
{
    private final Exhibitor exhibitor;
    private final ClusterState clusterState;
    private final UsState usState;

    FlexibleEnsembleBuilder(Exhibitor exhibitor, ClusterState clusterState)
    {
        this.exhibitor = exhibitor;
        this.clusterState = clusterState;
        usState = new UsState(exhibitor);
    }

    @SuppressWarnings("RedundantIfStatement")
    @Override
    public boolean newEnsembleNeeded()
    {
        if ( (usState.getUs() == null) || clusterState.hasDeadInstances() )
        {
            return true;
        }
        return false;
    }

    public ServerList  createPotentialServerList()
    {
        ServerList configuredServerList = clusterState.getConfiguredServerList();
        int existingMaxId = getExistingMaxId(configuredServerList);

        List<ServerSpec>        newList = Lists.newArrayList();

        Set<String> addedHostnames = Sets.newHashSet();
        for ( ServerStatus status : clusterState.getLiveInstances() )
        {
            ServerSpec spec = configuredServerList.getSpec(status.getHostname());
            if ( spec == null )
            {
                spec = new ServerSpec(status.getHostname(), ++existingMaxId, ServerType.STANDARD);
                addedHostnames.add(spec.getHostname());
            }
            newList.add(spec);
        }

        if ( usState.getUs() == null )
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
        if ( observerThreshold > 0 )
        {
            for ( int i = 0; (standardTypeCount >= observerThreshold) && (i < newList.size()); ++i )
            {
                ServerSpec      spec = newList.get(i);
                if ( addedHostnames.contains(spec.getHostname()) )  // i.e. don't change existing instances to observer
                {
                    newList.set(i, new ServerSpec(spec.getHostname(), spec.getServerId(), ServerType.OBSERVER));
                    --standardTypeCount;
                }
            }
        }

        return new ServerList(newList);
    }

    static int getExistingMaxId(ServerList existingList)
    {
        int                     existingMaxId = 0;
        for ( ServerSpec spec : existingList.getSpecs() )
        {
            if ( spec.getServerId() > existingMaxId )
            {
                existingMaxId = spec.getServerId();
            }
        }
        return existingMaxId;
    }
}
