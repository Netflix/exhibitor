package com.netflix.exhibitor.core.automanage;

import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.state.InstanceStateTypes;
import com.netflix.exhibitor.core.state.ServerList;
import com.netflix.exhibitor.core.state.ServerSpec;
import com.netflix.exhibitor.core.state.ServerType;
import com.netflix.exhibitor.core.state.UsState;
import java.util.List;
import java.util.Map;

class FixedEnsembleBuilder implements EnsembleBuilder
{
    private final Exhibitor exhibitor;
    private final ClusterState clusterState;
    private final UsState usState;
    private final int fixedEnsembleSize;

    FixedEnsembleBuilder(Exhibitor exhibitor, ClusterState clusterState)
    {
        this.exhibitor = exhibitor;
        this.clusterState = clusterState;
        usState = new UsState(exhibitor);
        fixedEnsembleSize = exhibitor.getConfigManager().getConfig().getInt(IntConfigs.AUTO_MANAGE_INSTANCES_FIXED_ENSEMBLE_SIZE);
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean newEnsembleNeeded()
    {
        if ( (usState.getUs() != null) )
        {
            // we're in the ensemble - nothing to do
            return false;
        }

        if ( clusterState.getConfiguredServerList().getSpecs().size() < fixedEnsembleSize )
        {
            return true;    // there's room, we can add ourselves in
        }

        if ( !clusterState.hasDeadInstances() )
        {
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Cannot add this instance to the ensemble as the ensemble is at the configured fixed ensemble size.");
            return false;
        }

        return true;
    }

    @Override
    public ServerList createPotentialServerList()
    {
        ServerList configuredServerList = clusterState.getConfiguredServerList();
        Map<ServerSpec, InstanceStateTypes> statusMap = clusterState.buildStatusMap();

        List<ServerSpec>        newList = Lists.newArrayList();
        for ( ServerSpec spec : configuredServerList.getSpecs() )
        {
            if ( statusMap.get(spec) != InstanceStateTypes.DOWN )
            {
                newList.add(spec);
            }
        }
        if ( newList.size() >= fixedEnsembleSize )
        {
            return configuredServerList;    // no room for us
        }

        int                 standardTypeCount = 0;
        for ( ServerSpec spec : newList )
        {
            if ( spec.getServerType() == ServerType.STANDARD )
            {
                ++standardTypeCount;
            }
        }

        int observerThreshold = exhibitor.getConfigManager().getConfig().getInt(IntConfigs.OBSERVER_THRESHOLD);
        ServerType serverType = ((observerThreshold > 0) && (standardTypeCount >= observerThreshold)) ? ServerType.OBSERVER : ServerType.STANDARD;

        int existingMaxId = FlexibleEnsembleBuilder.getExistingMaxId(configuredServerList);
        ServerSpec us = new ServerSpec(exhibitor.getThisJVMHostname(), existingMaxId + 1, serverType);
        newList.add(us);

        return new ServerList(newList);
    }
}
