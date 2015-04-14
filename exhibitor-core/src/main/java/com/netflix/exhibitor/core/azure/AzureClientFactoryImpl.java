package com.netflix.exhibitor.core.azure;


public class AzureClientFactoryImpl implements AzureClientFactory {
    @Override
    public AzureClient makeNewClient(AzureCredential credentials) throws Exception {
        return new AzureClientImpl(credentials);
    }
}
