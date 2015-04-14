package com.netflix.exhibitor.core.azure;

public interface AzureClientFactory {
    /**
     * Create a client with the given credentials
     *
     * @param credentials credentials
     * @return client
     * @throws Exception errors
     */
    public AzureClient makeNewClient(AzureCredential credentials) throws Exception;

}
