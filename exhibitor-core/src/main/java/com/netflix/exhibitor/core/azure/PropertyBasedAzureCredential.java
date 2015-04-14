package com.netflix.exhibitor.core.azure;

import org.apache.curator.utils.CloseableUtils;

import java.io.*;
import java.util.Properties;

public class PropertyBasedAzureCredential implements AzureCredential {
    private final String accountName;
    private final String accountKey;

    public static final String PROPERTY_AZURE_ACCOUNT_NAME = "com.netflix.exhibitor.azure.account-name";
    public static final String PROPERTY_AZURE_ACCOUNT_KEY = "com.netflix.exhibitor.azure.account-key";

    public PropertyBasedAzureCredential(File propertiesFile) throws IOException
    {
        this(loadProperties(propertiesFile));
    }

    public PropertyBasedAzureCredential(Properties properties)
    {
        accountName = properties.getProperty(PROPERTY_AZURE_ACCOUNT_NAME);
        accountKey = properties.getProperty(PROPERTY_AZURE_ACCOUNT_KEY);
    }

    @Override
    public String getAccountName()
    {
        return accountName;
    }

    public String getAccountKey()
    {
        return accountKey;
    }

    private static Properties loadProperties(File propertiesFile) throws IOException
    {
        Properties properties = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream(propertiesFile));
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
