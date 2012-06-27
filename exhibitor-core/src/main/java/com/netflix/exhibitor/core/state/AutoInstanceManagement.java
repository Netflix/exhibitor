package com.netflix.exhibitor.core.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.PseudoLock;
import com.netflix.exhibitor.core.config.StringConfigs;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AutoInstanceManagement implements Activity
{
    private final Exhibitor exhibitor;

    public AutoInstanceManagement(Exhibitor exhibitor)
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
        if ( exhibitor.getConfigManager().getConfig().getInt(IntConfigs.AUTO_MANAGE_INSTANCES) != 0 )
        {
            PseudoLock  lock = exhibitor.getConfigManager().newConfigBasedLock();
            if ( lock.lock(Exhibitor.AUTO_INSTANCE_MANAGEMENT_PERIOD_MS / 2, TimeUnit.MILLISECONDS) )
            {
                try
                {
                    if ( !exhibitor.getConfigManager().isRolling() )
                    {
                        doWork();
                    }
                }
                finally
                {
                    lock.unlock();
                }
            }
        }
        return true;
    }

    @VisibleForTesting
    protected void doWork() throws Exception
    {
        UsState usState = new UsState(exhibitor);

        exhibitor.getConfigManager().writeHeartbeat();
        if ( exhibitor.getMonitorRunningInstance().getCurrentInstanceState() != InstanceStateTypes.LATENT )
        {
            if ( usState.getUs() == null )
            {
                addUsIn(usState);
            }
        }

        checkForStaleInstances(usState);
    }

    private void checkForStaleInstances(UsState usState) throws Exception
    {
        List<ServerSpec>            newSpecList = Lists.newArrayList();
        boolean                     hasRemovals = false;
        for ( ServerSpec spec : usState.getServerList().getSpecs() )
        {
            long        elapsedSinceLastHeartbeat = usState.getUs().equals(spec) ? 0 : (System.currentTimeMillis() - exhibitor.getConfigManager().getLastHeartbeatForInstance(spec.getHostname()));
            if ( elapsedSinceLastHeartbeat <= exhibitor.getConfigManager().getConfig().getInt(IntConfigs.DEAD_INSTANCE_PERIOD_MS) )
            {
                newSpecList.add(spec);
            }
            else
            {
                exhibitor.getLog().add(ActivityLog.Type.INFO, "Potentially removing stale instance from servers list: " + spec.getHostname());
                hasRemovals = true;
            }
        }

        if ( hasRemovals )
        {
            List<String>    transformed = Lists.transform
            (
                newSpecList,
                new Function<ServerSpec, String>()
                {
                    @Override
                    public String apply(ServerSpec spec)
                    {
                        return spec.toSpecString();
                    }
                }
            );
            String          newSpec = Joiner.on(',').join(transformed);
            adjustConfig(exhibitor.getConfigManager().getConfig(), newSpec);
        }
    }

    private void addUsIn(UsState usState) throws Exception
    {
        int         maxServerId = 0;
        for ( ServerSpec spec : usState.getServerList().getSpecs() )
        {
            if ( spec.getServerId() > maxServerId )
            {
                maxServerId = spec.getServerId();
            }
        }

        int                     observerThreshold = exhibitor.getConfigManager().getConfig().getInt(IntConfigs.OBSERVER_THRESHOLD);
        int                     newUsIndex = usState.getServerList().getSpecs().size() + 1;
        ServerType              serverType = (newUsIndex >= observerThreshold) ? ServerType.OBSERVER : ServerType.STANDARD;

        InstanceConfig          currentConfig = exhibitor.getConfigManager().getConfig();
        String                  spec = currentConfig.getString(StringConfigs.SERVERS_SPEC);
        String                  thisValue = new ServerSpec(exhibitor.getThisJVMHostname(), maxServerId + 1, serverType).toSpecString();
        final String            newSpec = Joiner.on(',').skipNulls().join((spec.length() > 0) ? spec : null, thisValue);
        exhibitor.getLog().add(ActivityLog.Type.INFO, "Adding this instance to server list due to automatic instance management");
        adjustConfig(currentConfig, newSpec);
    }

    private void adjustConfig(final InstanceConfig currentConfig, final String newSpec) throws Exception
    {
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
        exhibitor.getConfigManager().startRollingConfig(newConfig); // if this fails due to an old config it's fine - it will just try again next time
    }
}
