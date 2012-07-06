package com.netflix.exhibitor.core.rest;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.exhibitor.core.state.RemoteInstanceRequestClient;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import javax.ws.rs.core.MediaType;
import java.net.URI;

class RemoteInstanceRequestClientImpl implements RemoteInstanceRequestClient
{
    private static final Client client = Client.create();
    private static final LoadingCache<URI, WebResource> webResources = CacheBuilder.newBuilder()
        .softValues()
        .build
            (
                new CacheLoader<URI, WebResource>()
                {
                    @Override
                    public WebResource load(URI remoteUri) throws Exception
                    {
                        return client.resource(remoteUri);
                    }
                }
            );

    @Override
    public <T> T getWebResource(URI remoteUri, MediaType type, Class<T> clazz) throws Exception
    {
        return webResources.get(remoteUri).accept(type).get(clazz);
    }
}
