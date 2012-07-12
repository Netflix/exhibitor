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

import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.StringConfigs;

public class RestartSignificantConfig
{
    private final Object        blob;

    public RestartSignificantConfig(InstanceConfig config)
    {
        StringBuilder       str = new StringBuilder();
        if ( config != null )
        {
            for ( StringConfigs v : StringConfigs.values() )
            {
                if ( v.isRestartSignificant() )
                {
                    str.append(config.getString(v));
                }
            }
            for ( IntConfigs v : IntConfigs.values() )
            {
                if ( v.isRestartSignificant() )
                {
                    str.append(config.getInt(v));
                }
            }
        }
        blob = str.toString();
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

        RestartSignificantConfig that = (RestartSignificantConfig)o;

        if ( !blob.equals(that.blob) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return blob.hashCode();
    }
}
