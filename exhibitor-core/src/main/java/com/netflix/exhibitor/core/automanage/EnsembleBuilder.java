package com.netflix.exhibitor.core.automanage;

import com.netflix.exhibitor.core.state.ServerList;

interface EnsembleBuilder
{
    public boolean newEnsembleNeeded();

    public ServerList createPotentialServerList();
}
