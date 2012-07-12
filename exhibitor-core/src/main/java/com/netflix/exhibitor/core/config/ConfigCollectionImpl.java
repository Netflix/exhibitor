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
import java.util.List;

class ConfigCollectionImpl extends ConfigCollectionBase
{
    private final InstanceConfig rootConfig;
    private final InstanceConfig rollingConfig;
    private final int rollingHostNamesIndex;
    private final ImmutableList<String> rollingHostNames;

    ConfigCollectionImpl(InstanceConfig rootConfig, InstanceConfig rollingConfig)
    {
        this(rootConfig, rollingConfig, ImmutableList.<String>of(), 0);
    }
    
    ConfigCollectionImpl(InstanceConfig rootConfig, InstanceConfig rollingConfig, List<String> rollingHostNames, int rollingHostNamesIndex)
    {
        this.rootConfig = rootConfig;
        this.rollingConfig = rollingConfig;
        this.rollingHostNamesIndex = rollingHostNamesIndex;
        this.rollingHostNames = ImmutableList.copyOf(rollingHostNames);
    }

    @Override
    public int getRollingHostNamesIndex()
    {
        return rollingHostNamesIndex;
    }

    @Override
    public InstanceConfig getRootConfig()
    {
        return rootConfig;
    }

    @Override
    public InstanceConfig getRollingConfig()
    {
        return (rollingConfig != null) ? rollingConfig : rootConfig;
    }

    @Override
    public List<String> getRollingHostNames()
    {
        return rollingHostNames;
    }
}
