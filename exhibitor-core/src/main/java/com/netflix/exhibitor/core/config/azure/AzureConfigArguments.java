package com.netflix.exhibitor.core.config.azure;

public class AzureConfigArguments {
    private final String container;
    private final String blobName;
    private final AzureConfigAutoManageLockArguments lockArguments;

    public AzureConfigArguments(String container, String blobName, AzureConfigAutoManageLockArguments lockArguments)
    {
        this.container = container;
        this.blobName = blobName;
        this.lockArguments = lockArguments;
    }

    public String getContainer()
    {
        return container;
    }

    public String getBlobName()
    {
        return blobName;
    }

    public AzureConfigAutoManageLockArguments getLockArguments()
    {
        return lockArguments;
    }
}
