package com.netflix.exhibitor.core.config.consul;

import java.util.Properties;

public class ConsulVersionedProperties {
    private final Properties properties;
    private final Long version;

    public ConsulVersionedProperties(Properties properties, Long version) {
        this.properties = properties;
        this.version = version;
    }

    public Properties getProperties() {
        return properties;
    }

    public Long getVersion() {
        return version;
    }
}
