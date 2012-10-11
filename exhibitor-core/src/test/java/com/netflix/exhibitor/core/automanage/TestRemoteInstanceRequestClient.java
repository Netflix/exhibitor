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

import com.google.common.io.Closeables;
import com.netflix.curator.test.InstanceSpec;
import org.testng.Assert;
import org.testng.annotations.Test;
import javax.ws.rs.core.MediaType;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestRemoteInstanceRequestClient
{
    @Test
    public void     testMissingServer() throws URISyntaxException
    {
        RemoteInstanceRequestClientImpl         client = new RemoteInstanceRequestClientImpl();
        try
        {
            // a non-existent port should generate an exception
            client.getWebResource(new URI("http://localhost:" + InstanceSpec.getRandomPort()), MediaType.WILDCARD_TYPE, Object.class);
        }
        catch ( Exception e )
        {
            Throwable cause = e.getCause();
            if ( cause == null )
            {
                cause = e;
            }
            Assert.assertTrue(cause instanceof ConnectException, cause.getClass().getName());
        }
        finally
        {
            Closeables.closeQuietly(client);
        }
    }

    @Test
    public void     testConnectionTimeout() throws Exception
    {
        int             port = InstanceSpec.getRandomPort();

        RemoteInstanceRequestClientImpl client = null;
        ServerSocket                    server = new ServerSocket(port, 0);
        try
        {
            client = new RemoteInstanceRequestClientImpl();
            client.getWebResource(new URI("http://localhost:" + port), MediaType.WILDCARD_TYPE, Object.class);
        }
        catch ( Exception e )
        {
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof SocketTimeoutException);
        }
        finally
        {
            Closeables.closeQuietly(client);
            server.close();
        }
    }
}
