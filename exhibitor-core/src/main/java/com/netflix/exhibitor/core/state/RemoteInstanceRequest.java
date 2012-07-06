package com.netflix.exhibitor.core.state;

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.rest.ClusterResource;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class RemoteInstanceRequest
{
    private final Exhibitor exhibitor;
    private final String hostname;

    private static final String clusterResourcePath;
    static
    {
        Path annotation = ClusterResource.class.getAnnotation(Path.class);
        clusterResourcePath = annotation.value();
    }

    public RemoteInstanceRequest(Exhibitor exhibitor, String hostname)
    {
        this.exhibitor = exhibitor;
        this.hostname = hostname;
    }

    public static class Result
    {
        public final String      remoteResponse;
        public final String      errorMessage;

        public Result(String remoteResponse, String errorMessage)
        {
            this.remoteResponse = remoteResponse;
            this.errorMessage = errorMessage;
        }

        @Override
        public String toString()
        {
            return "Result{" +
                "remoteResponse='" + remoteResponse + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
        }
    }

    public String getHostname()
    {
        return hostname;
    }

    public Result         makeRequest(RemoteInstanceRequestClient client, String methodName, Object... values)
    {
        String      remoteResponse;
        String      errorMessage;
        {
            try
            {
                URI remoteUri = UriBuilder
                    .fromPath(getPath())
                    .scheme(exhibitor.getRestScheme())
                    .host(hostname)
                    .port(exhibitor.getRestPort())
                    .path(ClusterResource.class, methodName)
                    .build(values);

                remoteResponse = client.getWebResource(remoteUri, MediaType.APPLICATION_JSON_TYPE, String.class);
                errorMessage = "";
            }
            catch ( Exception e )
            {
                remoteResponse = "{}";
                errorMessage = e.getMessage();
                if ( errorMessage == null )
                {
                    errorMessage = "Unknown";
                }
            }
        }

        return new Result(remoteResponse, errorMessage);
    }

    private String getPath()
    {
        StringBuilder       thisPath = new StringBuilder();
        if ( exhibitor.getRestPath() != null )
        {
            if ( !exhibitor.getRestPath().startsWith("/") )
            {
                thisPath.append("/");
            }
            thisPath.append(exhibitor.getRestPath());
            if ( !exhibitor.getRestPath().endsWith("/") )
            {
                thisPath.append("/");
            }
        }
        else
        {
            thisPath.append("/");
        }

        thisPath.append(clusterResourcePath);
        return thisPath.toString();
    }
}
