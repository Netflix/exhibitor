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

package com.netflix.exhibitor.core.state;

import com.sun.jersey.api.client.WebResource;
import javax.ws.rs.core.MediaType;
import java.net.URI;

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
