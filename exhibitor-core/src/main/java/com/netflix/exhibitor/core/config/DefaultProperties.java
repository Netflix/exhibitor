/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.exhibitor.core.config;

import com.google.common.collect.Maps;
import com.netflix.exhibitor.core.backup.BackupConfigSpec;
import com.netflix.exhibitor.core.backup.BackupProvider;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * The default values
 */
public class DefaultProperties
{
    public static int asInt(String s)
    {
        try
        {
            return Integer.parseInt(s);
        }
        catch ( NumberFormatException e )
        {
            // ignore
        }
        return 0;
    }

    /**
     * Return the default properties given an optional backup provider
     *
     * @param provider provider or null
     * @return default properties
     */
    public static Properties get(BackupProvider provider)
    {
        return getFromInstanceConfig(newDefaultInstanceConfig(provider));
    }

    /**
     * Return the default properties given an instance config
     *
     * @param defaultInstanceConfig the default properties as an object
     * @return default properties
     */
    public static Properties getFromInstanceConfig(InstanceConfig defaultInstanceConfig)
    {
        PropertyBasedInstanceConfig     config = new PropertyBasedInstanceConfig(new ConfigCollectionImpl(defaultInstanceConfig, null));
        return config.getProperties();
    }

    /**
     * Return the standard default properties as an instance config
     *
     * @param provider backup provider or null
     * @return default properties instance
     */
    public static InstanceConfig newDefaultInstanceConfig(BackupProvider provider)
    {
        Map<String, String>             backupDefaultValues = Maps.newHashMap();
        if ( provider != null )
        {
            for ( BackupConfigSpec spec : provider.getConfigs() )
            {
                backupDefaultValues.put(spec.getKey(), spec.getDefaultValue());
            }
        }
        final String                    backupExtraValue = new EncodedConfigParser(backupDefaultValues).toEncoded();

        return new InstanceConfig()
        {
            @Override
            public String getString(StringConfigs config)
            {
                switch ( config )
                {
                    case ZOO_CFG_EXTRA:
                    {
                        return "syncLimit=5&tickTime=2000&initLimit=10";
                    }

                    case BACKUP_EXTRA:
                    {
                        return backupExtraValue;
                    }
                }
                return "";
            }

            @Override
            public int getInt(IntConfigs config)
            {
                switch ( config )
                {
                    case CLIENT_PORT:
                    {
                        return 2181;
                    }

                    case CONNECT_PORT:
                    {
                        return 2888;
                    }

                    case ELECTION_PORT:
                    {
                        return 3888;
                    }

                    case CHECK_MS:
                    {
                        return (int)TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
                    }

                    case CLEANUP_PERIOD_MS:
                    {
                        return (int)TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS);
                    }

                    case CLEANUP_MAX_FILES:
                    {
                        return 3;
                    }

                    case BACKUP_PERIOD_MS:
                    {
                        return (int)TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
                    }

                    case BACKUP_MAX_STORE_MS:
                    {
                        return (int)TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
                    }

                    case DEAD_INSTANCE_PERIOD_MS:
                    {
                        return (int)TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);
                    }

                    case OBSERVER_THRESHOLD:
                    {
                        return 5;
                    }

                }
                return 0;
            }
        };
    }

    private DefaultProperties()
    {
    }
}
