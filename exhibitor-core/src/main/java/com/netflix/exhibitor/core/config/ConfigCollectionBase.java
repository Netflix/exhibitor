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

import java.util.List;

abstract class ConfigCollectionBase implements ConfigCollection, RollingConfigState
{
    @Override
    public final InstanceConfig getConfigForThisInstance(String hostname)
    {
        return (isRolling() && getRollingHostNames().contains(hostname)) ? getRollingConfig() : getRootConfig();
    }

    @Override
    public final boolean isRolling()
    {
        return (getRollingHostNames().size() > 0);
    }

    @Override
    public final String getRollingStatus()
    {
        if ( !isRolling() )
        {
            return "n/a";
        }

        List<String>    rollingHostNames = getRollingHostNames();
        int             rollingHostNamesIndex = getRollingHostNamesIndex();
        String          currentRollingHostname = rollingHostNames.get(rollingHostNamesIndex);

        StringBuilder   status = new StringBuilder("Applying to \"").append(currentRollingHostname).append("\"");
        if ( (rollingHostNamesIndex + 1) < rollingHostNames.size() )
        {
            status.append(" (next will be \"").append(rollingHostNames.get(rollingHostNamesIndex + 1)).append("\")");
        }

        return status.toString();
    }

    @Override
    public final int getRollingPercentDone()
    {
        if ( isRolling() )
        {
            return Math.max(1, (100 * getRollingHostNamesIndex()) / getRollingHostNames().size());
        }
        return 0;
    }

    @Override
    public final RollingConfigState getRollingConfigState()
    {
        return this;
    }
}
