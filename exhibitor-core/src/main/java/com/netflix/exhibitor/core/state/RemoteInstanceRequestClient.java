package com.netflix.exhibitor.core.state;

import com.sun.jersey.api.client.WebResource;
import javax.ws.rs.core.MediaType;
import java.net.URI;

/**
 * Abstraction for getting a JAX-RS WebResource
 */
public interface RemoteInstanceRequestClient
{
    /**
     * Return the WebResource (usually cached) for the given URI
     *
     * @param remoteUri URI
     * @param type media type
     * @param clazz resource class
     * @return WebResource
     * @throws Exception errors
     */
    public <T> T   getWebResource(URI remoteUri, MediaType type, Class<T> clazz) throws Exception;
}
