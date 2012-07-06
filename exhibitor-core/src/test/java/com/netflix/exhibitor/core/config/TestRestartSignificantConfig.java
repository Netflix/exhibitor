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
