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

package com.netflix.exhibitor.core.s3;

import com.amazonaws.ClientConfiguration;
import org.apache.curator.utils.CloseableUtils;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyBasedS3ClientConfig implements S3ClientConfig
{
    private final String proxyHost;
    private final int proxyPort;
    private final String proxyUsername;
    private final String proxyPassword;

    public static final String PROPERTY_S3_PROXY_HOST = "com.netflix.exhibitor.s3.proxy-host";
    public static final String PROPERTY_S3_PROXY_PORT = "com.netflix.exhibitor.s3.proxy-port";
    public static final String PROPERTY_S3_PROXY_USERNAME = "com.netflix.exhibitor.s3.proxy-username";
    public static final String PROPERTY_S3_PROXY_PASSWORD = "com.netflix.exhibitor.s3.proxy-password";

    public PropertyBasedS3ClientConfig(File propertiesFile) throws IOException
    {
        this(loadProperties(propertiesFile));
    }

    public PropertyBasedS3ClientConfig(Properties properties)
    {
        proxyHost = properties.getProperty(PROPERTY_S3_PROXY_HOST);
        proxyPort = Integer.parseInt(properties.getProperty(PROPERTY_S3_PROXY_PORT));
        proxyUsername = properties.getProperty(PROPERTY_S3_PROXY_USERNAME);
        proxyPassword = properties.getProperty(PROPERTY_S3_PROXY_PASSWORD);
    }

    @Override
    public ClientConfiguration getAWSClientConfig()
    {
        ClientConfiguration awsClientConfig = new ClientConfiguration();
        awsClientConfig.setProxyHost(proxyHost);
        awsClientConfig.setProxyPort(proxyPort);

        if ( proxyUsername != null )
        {
            awsClientConfig.setProxyUsername(proxyUsername);
        }

        if ( proxyPassword != null )
        {
            awsClientConfig.setProxyPassword(proxyPassword);
        }
        return awsClientConfig;
    }

    private static Properties loadProperties(File propertiesFile) throws IOException
    {
        Properties      properties = new Properties();
        InputStream     in = new BufferedInputStream(new FileInputStream(propertiesFile));
        try
        {
            properties.load(in);
        }
        finally
        {
            CloseableUtils.closeQuietly(in);
        }
        return properties;
    }
}
