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

package com.netflix.exhibitor.core.automanage;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.exhibitor.core.RemoteConnectionConfiguration;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.ClientFilter;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.SocketException;
import java.net.URI;

public class RemoteInstanceRequestClientImpl implements RemoteInstanceRequestClient
{
    private final Client client;
    private final LoadingCache<URI, WebResource> webResources = CacheBuilder.newBuilder()
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

    public RemoteInstanceRequestClientImpl(RemoteConnectionConfiguration configuration)
    {
        client = Client.create();
        client.setConnectTimeout(configuration.getConnectionTimeoutMs());
        client.setReadTimeout(configuration.getReadTimeoutMs());
        for ( ClientFilter filter : configuration.getFilters() )
        {
            client.addFilter(filter);
        }
    }

    @Override
    public <T> T getWebResource(URI remoteUri, MediaType type, Class<T> clazz) throws Exception
    {
        try
        {
            return webResources.get(remoteUri).accept(type).get(clazz);
        }
        catch ( Exception e )
        {
            if ( e.getCause() instanceof SocketException )
            {
                throw (SocketException)e.getCause();
            }

            throw e;
        }
    }

    @Override
    public void close() throws IOException
    {
        client.destroy();
    }
}
