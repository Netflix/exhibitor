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

package com.netflix.exhibitor.core.automanage;

import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.config.ConfigCollection;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.PseudoLock;
import com.netflix.exhibitor.core.config.RollingConfigState;
import com.netflix.exhibitor.core.config.StringConfigs;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class MockConfigProvider implements ConfigProvider
{
    private volatile ConfigCollection       collection = new PropertyBasedInstanceConfig(new Properties(), new Properties());
    private final AtomicInteger             version = new AtomicInteger(0);

    void setConfig(final StringConfigs type, final String value)
    {
        final InstanceConfig config = collection.getRootConfig();
        final InstanceConfig newConfig = new InstanceConfig()
        {
            @Override
            public String getString(StringConfigs configType)
            {
                if ( configType == type )
                {
                    return value;
                }
                return config.getString(configType);
            }

            @Override
            public int getInt(IntConfigs configType)
            {
                return config.getInt(configType);
            }
        };
        collection = new ConfigCollection()
        {
            @Override
            public InstanceConfig getConfigForThisInstance(String hostname)
            {
                return getRootConfig();
            }

            @Override
            public InstanceConfig getRootConfig()
            {
                return newConfig;
            }

            @Override
            public InstanceConfig getRollingConfig()
            {
                return null;
            }

            @Override
            public boolean isRolling()
            {
                return false;
            }

            @Override
            public RollingConfigState getRollingConfigState()
            {
                return null;
            }
        };
    }

    void setConfig(final IntConfigs type, final int value)
    {
        final InstanceConfig config = collection.getRootConfig();
        final InstanceConfig newConfig = new InstanceConfig()
        {
            @Override
            public String getString(StringConfigs configType)
            {
                return config.getString(configType);
            }

            @Override
            public int getInt(IntConfigs configType)
            {
                if ( configType == type )
                {
                    return value;
                }
                return config.getInt(configType);
            }
        };
        collection = new ConfigCollection()
        {
            @Override
            public InstanceConfig getConfigForThisInstance(String hostname)
            {
                return getRootConfig();
            }

            @Override
            public InstanceConfig getRootConfig()
            {
                return newConfig;
            }

            @Override
            public InstanceConfig getRollingConfig()
            {
                return null;
            }

            @Override
            public boolean isRolling()
            {
                return false;
            }

            @Override
            public RollingConfigState getRollingConfigState()
            {
                return null;
            }
        };
    }

    @Override
    public void start() throws Exception
    {
    }

    @Override
    public LoadedInstanceConfig loadConfig() throws Exception
    {
        version.incrementAndGet();
        return new LoadedInstanceConfig(collection, version.get())
        {
            @Override
            public ConfigCollection getConfig()
            {
                return collection;
            }
        };
    }

    @Override
    public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareVersion) throws Exception
    {
        if ( compareVersion != version.get() )
        {
            return null;
        }

        collection = config;
        return new LoadedInstanceConfig(config, compareVersion)
        {
            @Override
            public ConfigCollection getConfig()
            {
                return collection;
            }
        };
    }

    @Override
    public PseudoLock newPseudoLock() throws Exception
    {
        return new PseudoLock()
        {
            @Override
            public boolean lock(ActivityLog log, long maxWait, TimeUnit unit) throws Exception
            {
                return true;
            }

            @Override
            public void unlock() throws Exception
            {
            }
        };
    }

    @Override
    public void close() throws IOException
    {
    }
}
