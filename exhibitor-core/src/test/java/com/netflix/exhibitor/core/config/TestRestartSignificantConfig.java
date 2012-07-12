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

import com.netflix.exhibitor.core.state.RestartSignificantConfig;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Properties;

public class TestRestartSignificantConfig
{
    @Test
    public void     testBasic()
    {
        final InstanceConfig        config = new PropertyBasedInstanceConfig(new Properties(), new Properties()).getRootConfig();
        RestartSignificantConfig    check = new RestartSignificantConfig(config);
        Assert.assertEquals(check, check);
        InstanceConfig              anotherConfig = new InstanceConfig()
        {
            @Override
            public String getString(StringConfigs v)
            {
                return config.getString(v);
            }

            @Override
            public int getInt(IntConfigs v)
            {
                return config.getInt(v);
            }
        };

        Assert.assertEquals(check, new RestartSignificantConfig(anotherConfig));

        InstanceConfig              stillAnotherConfig = new InstanceConfig()
        {
            @Override
            public String getString(StringConfigs v)
            {
                if ( v == StringConfigs.JAVA_ENVIRONMENT )
                {
                    return config.getString(v) + " a change of some kind";
                }
                return config.getString(v);
            }

            @Override
            public int getInt(IntConfigs v)
            {
                return config.getInt(v);
            }
        };
        Assert.assertNotSame(check, new RestartSignificantConfig(stillAnotherConfig));
    }
}
