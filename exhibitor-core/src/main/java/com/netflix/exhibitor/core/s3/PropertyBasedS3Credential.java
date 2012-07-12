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

import com.google.common.io.Closeables;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyBasedS3Credential implements S3Credential
{
    private final String accessKeyId;
    private final String accessSecretKey;

    public static final String PROPERTY_S3_KEY_ID = "com.netflix.exhibitor.s3.access-key-id";
    public static final String PROPERTY_S3_SECRET_KEY = "com.netflix.exhibitor.s3.access-secret-key";

    public PropertyBasedS3Credential(File propertiesFile) throws IOException
    {
        this(loadProperties(propertiesFile));
    }

    public PropertyBasedS3Credential(Properties properties)
    {
        accessKeyId = properties.getProperty(PROPERTY_S3_KEY_ID);
        accessSecretKey = properties.getProperty(PROPERTY_S3_SECRET_KEY);
    }

    @Override
    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    @Override
    public String getSecretAccessKey()
    {
        return accessSecretKey;
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
            Closeables.closeQuietly(in);
        }
        return properties;
    }
}
