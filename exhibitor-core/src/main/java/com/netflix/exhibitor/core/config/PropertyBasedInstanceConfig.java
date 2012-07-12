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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PropertyBasedInstanceConfig extends ConfigCollectionBase
{
    private final Properties properties;
    private final Properties defaults;
    private final List<String> rollingHostNames;
    private final WrappedInstanceConfig rootConfig;
    private final WrappedInstanceConfig rollingConfig;

    @VisibleForTesting
    static final String ROOT_PROPERTY_PREFIX = "com.netflix.exhibitor.";

    @VisibleForTesting
    static final String ROLLING_PROPERTY_PREFIX = "com.netflix.exhibitor-rolling.";

    private static final String PROPERTY_ROLLING_HOSTNAMES = "com.netflix.exhibitor-rolling-hostnames";
    private static final String PROPERTY_ROLLING_HOSTNAMES_INDEX = "com.netflix.exhibitor-rolling-hostnames-index";

    /**
     * Used to wrap an existing config
     *
     * @param configCollection source config
     */
    public PropertyBasedInstanceConfig(ConfigCollection configCollection)
    {
        this(buildPropertiesFromCollection(configCollection), new Properties());
    }

    /**
     * @param properties the properties
     * @param defaults default values
     */
    public PropertyBasedInstanceConfig(Properties properties, Properties defaults)
    {
        this.properties = properties;
        this.defaults = defaults;

        rollingHostNames = internalGetRollingHostNames();
        rootConfig = new WrappedInstanceConfig(ROOT_PROPERTY_PREFIX);
        rollingConfig = new WrappedInstanceConfig(ROLLING_PROPERTY_PREFIX);
    }

    public Properties getProperties()
    {
        Properties      copy = new Properties();
        copy.putAll(properties);
        return copy;
    }

    @Override
    public InstanceConfig getRootConfig()
    {
        return rootConfig;
    }

    @Override
    public InstanceConfig getRollingConfig()
    {
        return rollingConfig;
    }

    @Override
    public List<String> getRollingHostNames()
    {
        return rollingHostNames;
    }

    @Override
    public int getRollingHostNamesIndex()
    {
        return DefaultProperties.asInt(properties.getProperty(PROPERTY_ROLLING_HOSTNAMES_INDEX, "0"));
    }

    @VisibleForTesting
    static String toName(Enum e, String prefix)
    {
        String  s = e.name();
        s = s.replace('_', '-');
        s = s.toLowerCase();
        return prefix + s;
    }

    private static Properties buildPropertiesFromCollection(ConfigCollection collection)
    {
        Properties      properties = new Properties();
        buildPropertiesFromSource(collection.getRootConfig(), properties, ROOT_PROPERTY_PREFIX);
        buildPropertiesFromSource(collection.getRollingConfig(), properties, ROLLING_PROPERTY_PREFIX);

        StringBuilder   rollingNames = new StringBuilder();
        for ( String s : collection.getRollingConfigState().getRollingHostNames() )
        {
            if ( rollingNames.length() > 0 )
            {
                rollingNames.append(",");
            }
            rollingNames.append(s);
        }
        properties.setProperty(PROPERTY_ROLLING_HOSTNAMES, rollingNames.toString());
        properties.setProperty(PROPERTY_ROLLING_HOSTNAMES_INDEX, Integer.toString(collection.getRollingConfigState().getRollingHostNamesIndex()));

        return properties;
    }

    private static void buildPropertiesFromSource(InstanceConfig source, Properties properties, String prefix)
    {
        for ( StringConfigs config : StringConfigs.values() )
        {
            properties.setProperty(toName(config, prefix), source.getString(config));
        }
        for ( IntConfigs config : IntConfigs.values() )
        {
            properties.setProperty(toName(config, prefix), Integer.toString(source.getInt(config)));
        }
    }

    private List<String> internalGetRollingHostNames()
    {
        String  hostnames = properties.getProperty(PROPERTY_ROLLING_HOSTNAMES, null);
        if ( (hostnames != null) && (hostnames.trim().length() > 0) )
        {
            Iterables.transform
            (
                Arrays.asList(hostnames.split(",")),
                new Function<String, String>()
                {
                    @Override
                    public String apply(String str)
                    {
                        return str.trim();
                    }
                }
            );
            return ImmutableList.copyOf(Arrays.asList(hostnames.split(",")));
        }
        return ImmutableList.of();
    }

    private class WrappedInstanceConfig implements InstanceConfig
    {
        private final String prefix;

        public WrappedInstanceConfig(String prefix)
        {
            this.prefix = prefix;
        }

        @Override
        public String getString(StringConfigs config)
        {
            String  propertyName = toName(config, prefix);
            return properties.getProperty(propertyName, defaults.getProperty(propertyName, ""));
        }

        @Override
        public int getInt(IntConfigs config)
        {
            String propertyName = toName(config, prefix);
            return DefaultProperties.asInt(properties.getProperty(propertyName, defaults.getProperty(propertyName, "0")));
        }
    }
}
