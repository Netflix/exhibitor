package com.netflix.exhibitor.core.config.consul;

import com.google.common.net.HostAndPort;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.StringConfigs;
import com.orbitz.consul.Consul;
import com.orbitz.consul.model.session.SessionInfo;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.List;
import java.util.Properties;


public class TestConsulConfigProvider {
    private Timing timing;
    private ConsulProcess consul;
    private Consul client;

    @BeforeClass
    public void setupClass() throws Exception {
        consul = ConsulStarterBuilder.consulStarter().build().start();
    }

    @AfterClass
    public void tearDownClass()
    {
        consul.close();
    }

    @BeforeMethod
    public void setup() throws Exception {
        timing = new Timing();

        consul.reset();
        client = Consul.builder()
                .withHostAndPort(HostAndPort.fromParts("localhost", consul.getHttpPort()))
                .build();
    }

    @Test
    public void testBasic() throws Exception {
        ConsulConfigProvider config = new ConsulConfigProvider(client, "prefix", new Properties());

        try {
            config.start();
            config.loadConfig();

            Properties properties = new Properties();
            properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.ZOO_CFG_EXTRA, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), "1,2,3");
            config.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties()), 0);

            timing.sleepABit();

            LoadedInstanceConfig instanceConfig = config.loadConfig();
            Assert.assertEquals(instanceConfig.getConfig().getRootConfig().getString(StringConfigs.ZOO_CFG_EXTRA), "1,2,3");

            List<SessionInfo> sessions = client.sessionClient().listSessions();
            Assert.assertEquals(sessions.size(), 0, "Consul session still exists!");
        }
        finally {
            CloseableUtils.closeQuietly(config);
        }
    }
}