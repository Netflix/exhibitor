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

package com.netflix.exhibitor.core.config.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.config.ConfigCollection;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.PseudoLock;
import com.netflix.exhibitor.core.s3.S3Client;
import com.netflix.exhibitor.core.s3.S3ClientFactory;
import com.netflix.exhibitor.core.s3.S3Credential;
import com.netflix.exhibitor.core.s3.S3Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

public class S3ConfigProvider implements ConfigProvider
{
    private final S3ConfigArguments arguments;
    private final S3Client s3Client;
    private final String hostname;
    private final Properties defaults;

    private static final String         HEARTBEAT_CONTENT = "heartbeat";

    public S3ConfigProvider(S3ClientFactory factory, S3Credential credential, S3ConfigArguments arguments, String hostname) throws Exception
    {
        this(factory, credential, arguments, hostname, new Properties());
    }

    public S3ConfigProvider(S3ClientFactory factory, S3Credential credential, S3ConfigArguments arguments, String hostname, Properties defaults) throws Exception
    {
        this.arguments = arguments;
        this.hostname = hostname;
        this.defaults = defaults;
        s3Client = factory.makeNewClient(credential);
    }

    public S3Client getS3Client()
    {
        return s3Client;
    }

    @Override
    public void start() throws Exception
    {
        // NOP
    }

    @Override
    public void close() throws IOException
    {
        s3Client.close();
    }

    @Override
    public PseudoLock newPseudoLock() throws Exception
    {
        return new S3PseudoLock
        (
            s3Client,
            arguments.getBucket(),
            arguments.getLockArguments().getPrefix(),
            arguments.getLockArguments().getTimeoutMs(),
            arguments.getLockArguments().getPollingMs(),
            arguments.getLockArguments().getSettlingMs()
        );
    }

    @Override
    public void writeInstanceHeartbeat() throws Exception
    {
        Calendar                now = Calendar.getInstance();
        Calendar                tomorrow = Calendar.getInstance();
        tomorrow.add(Calendar.DAY_OF_MONTH, 1);

        ObjectMetadata          metadata = new ObjectMetadata();
        metadata.setContentLength(HEARTBEAT_CONTENT.length());
        metadata.setLastModified(now.getTime());
        metadata.setExpirationTime(tomorrow.getTime());
        PutObjectRequest        request = new PutObjectRequest(arguments.getBucket(), getHeartbeatKey(hostname), new ByteArrayInputStream(HEARTBEAT_CONTENT.getBytes()), metadata);

        s3Client.putObject(request);
    }

    @Override
    public void clearInstanceHeartbeat() throws Exception
    {
        try
        {
            s3Client.deleteObject(arguments.getBucket(), getHeartbeatKey(hostname));
        }
        catch ( AmazonServiceException ignore )
        {
            // ignore
        }
    }

    private String getHeartbeatKey(String instanceHostname)
    {
        return arguments.getHeartbeatKeyPrefix() + instanceHostname;
    }

    @Override
    public boolean isHeartbeatAliveForInstance(String instanceHostname, int deadInstancePeriodMs) throws Exception
    {
        long    lastHeartbeatForInstance = getLastHeartbeatForInstance(instanceHostname);
        long    elapsedSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatForInstance;
        return elapsedSinceLastHeartbeat <= deadInstancePeriodMs;
    }

    private long getLastHeartbeatForInstance(String instanceHostname) throws Exception
    {
        try
        {
            ObjectMetadata  metadata = s3Client.getObjectMetadata(arguments.getBucket(), getHeartbeatKey(instanceHostname));
            if ( metadata != null )
            {
                return metadata.getLastModified().getTime();
            }
        }
        catch ( AmazonServiceException ignore )
        {
            // treat this as a missing object
        }

        return 0;
    }

    @Override
    public LoadedInstanceConfig loadConfig() throws Exception
    {
        Date        lastModified;
        Properties  properties = new Properties();
        S3Object    object = getConfigObject();
        if ( object != null )
        {
            try
            {
                lastModified = object.getObjectMetadata().getLastModified();
                properties.load(object.getObjectContent());
            }
            finally
            {
                Closeables.closeQuietly(object.getObjectContent());
            }
        }
        else
        {
            lastModified = new Date(0L);
        }

        PropertyBasedInstanceConfig config = new PropertyBasedInstanceConfig(properties, defaults);
        return new LoadedInstanceConfig(config, lastModified.getTime());
    }

    @Override
    public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareLastModified) throws Exception
    {
        {
            ObjectMetadata                  metadata = getConfigMetadata();
            if ( metadata != null )
            {
                Date                            lastModified = metadata.getLastModified();
                if ( lastModified.getTime() != compareLastModified )
                {
                    return null;    // apparently there's no atomic way to do this with S3 so this will have to do
                }
            }
        }

        PropertyBasedInstanceConfig     propertyBasedInstanceConfig = new PropertyBasedInstanceConfig(config);
        ByteArrayOutputStream           out = new ByteArrayOutputStream();
        propertyBasedInstanceConfig.getProperties().store(out, "Auto-generated by Exhibitor " + hostname);

        byte[]                          bytes = out.toByteArray();
        ObjectMetadata                  metadata = S3Utils.simpleUploadFile(s3Client, bytes, arguments.getBucket(), arguments.getKey());

        return new LoadedInstanceConfig(propertyBasedInstanceConfig, metadata.getLastModified().getTime());
    }

    private ObjectMetadata getConfigMetadata() throws Exception
    {
        try
        {
            ObjectMetadata metadata = s3Client.getObjectMetadata(arguments.getBucket(), arguments.getKey());
            if ( metadata.getContentLength() > 0 )
            {
                return metadata;
            }
        }
        catch ( AmazonS3Exception e )
        {
            if ( !isNotFoundError(e) )
            {
                throw e;
            }
        }
        return null;
    }

    private S3Object getConfigObject() throws Exception
    {
        try
        {
            S3Object object = s3Client.getObject(arguments.getBucket(), arguments.getKey());
            if ( object.getObjectMetadata().getContentLength() > 0 )
            {
                return object;
            }
        }
        catch ( AmazonS3Exception e )
        {
            if ( !isNotFoundError(e) )
            {
                throw e;
            }
        }
        return null;
    }

    private boolean isNotFoundError(AmazonS3Exception e)
    {
        return (e.getStatusCode() == 404) || (e.getStatusCode() == 403);
    }
}
