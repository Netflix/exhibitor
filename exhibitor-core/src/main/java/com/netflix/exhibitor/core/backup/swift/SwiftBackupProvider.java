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

package com.netflix.exhibitor.core.backup.swift;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.backup.BackupConfigSpec;
import com.netflix.exhibitor.core.backup.BackupMetaData;
import com.netflix.exhibitor.core.backup.BackupProvider;
import com.netflix.exhibitor.core.backup.BackupStream;

import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteArrayPayload;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.domain.ObjectList;
import org.jclouds.openstack.swift.v1.domain.SwiftObject;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.netflix.exhibitor.core.config.DefaultProperties.asInt;

public class SwiftBackupProvider implements BackupProvider
{
    private final SwiftApi  swiftApi;

	private static final BackupConfigSpec CONFIG_THROTTLE = new BackupConfigSpec("throttle", "Throttle (bytes/ms)", "Data throttling. Maximum bytes per millisecond.", Integer.toString(1024 * 1024), BackupConfigSpec.Type.INTEGER);
    private static final BackupConfigSpec CONFIG_CONTAINER= new BackupConfigSpec("container-name", "Swift Container Name", "The Swfit containerto use", "", BackupConfigSpec.Type.STRING);
    private static final BackupConfigSpec CONFIG_KEY_PREFIX = new BackupConfigSpec("key-prefix", "Swfit Key Prefix", "The prefix for Swift backup keys", "exhibitor-backup", BackupConfigSpec.Type.STRING);
    private static final BackupConfigSpec CONFIG_MAX_RETRIES = new BackupConfigSpec("max-retries", "Max Retries", "Maximum retries when uploading/downloading S3 data", "3", BackupConfigSpec.Type.INTEGER);
    private static final BackupConfigSpec CONFIG_RETRY_SLEEP_MS = new BackupConfigSpec("retry-sleep-ms", "Retry Sleep (ms)", "Sleep time in milliseconds when retrying", "1000", BackupConfigSpec.Type.INTEGER);

    private static final List<BackupConfigSpec>     CONFIGS = Arrays.asList(CONFIG_THROTTLE, CONFIG_CONTAINER, CONFIG_KEY_PREFIX, CONFIG_MAX_RETRIES, CONFIG_RETRY_SLEEP_MS);
    
    private static final int        MIN_SWIFT_PART_SIZE = 5 * (1024 * 1024);

    @VisibleForTesting
    static final String       SEPARATOR = "/";
    private static final String       SEPARATOR_REPLACEMENT = "_";

    /**
     * 
     * @param swiftApi the SwiftApi
     * @throws Exception
     */
    public SwiftBackupProvider(SwiftApi  swiftApi) throws Exception
    {
        this.swiftApi = swiftApi;
    }


    @Override
    public List<BackupConfigSpec> getConfigs()
    {
        return CONFIGS;
    }

    @Override
    public boolean isValidConfig(Exhibitor exhibitor, Map<String, String> configValues)
    {
        String containerName = (configValues != null) ? configValues.get(CONFIG_CONTAINER.getKey()) : null;
        return (containerName != null) && (containerName.trim().length() > 0);
    }
    
    private ObjectApi getObjectApi(String containerName)
    {
    		String region = swiftApi.getConfiguredRegions().iterator().next();
    		
    		ContainerApi containerApi = swiftApi.getContainerApi(region);
    		if (containerApi.get(containerName) ==null)
    			containerApi.create(containerName);
    		
    	    return swiftApi.getObjectApi(region, containerName);

    }

    @Override
    public UploadResult uploadBackup(Exhibitor exhibitor, BackupMetaData backup, File source, final Map<String, String> configValues) throws Exception
    {
    	System.out.println("uploadBackup:"+backup);
        List<BackupMetaData>    availableBackups = getAvailableBackups(exhibitor, configValues);
        if ( availableBackups.contains(backup) )
        {
            return UploadResult.DUPLICATE;
        }

        String      key = toKey(backup, configValues);
        ObjectApi	objectApi = getObjectApi(configValues.get(CONFIG_CONTAINER.getKey()));

        byte[]          bytes = Files.toByteArray(source);
		Payload payload =  new ByteArrayPayload(bytes);
	    objectApi.put(key, payload);
	    	
	    //TODO find a way to do multiparts
	    
        UploadResult        result = UploadResult.SUCCEEDED;
        for ( BackupMetaData existing : availableBackups )
        {
            if ( existing.getName().equals(backup.getName()) )
            {
                deleteBackup(exhibitor, existing, configValues);
                result = UploadResult.REPLACED_OLD_VERSION;
            }
        }
        return result;
    }


    @Override
    public BackupStream getBackupStream(Exhibitor exhibitor, BackupMetaData backup, Map<String, String> configValues) throws Exception
    {
    	System.out.println("getBackupStream:"+backup);

    	long            startMs = System.currentTimeMillis();
        RetryPolicy     retryPolicy = makeRetryPolicy(configValues);
        int             retryCount = 0;
        ObjectApi	    objectApi = getObjectApi(configValues.get(CONFIG_CONTAINER.getKey()));
        SwiftObject     object = null;
        while ( object == null )
        {
            try
            {
                object = objectApi.get(toKey(backup, configValues));
            }
            catch ( Exception e)
            {
            	e.printStackTrace();
            	/*TODO any fastpath on failure?
            	if (some condition)
            	{
                exhibitor.getLog().add(ActivityLog.Type.ERROR, "Swift client error: " + ActivityLog.getExceptionMessage(e));
                return null;
            	}*/
                if ( !retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
                {
                    exhibitor.getLog().add(ActivityLog.Type.ERROR, "Retries exhausted: " + ActivityLog.getExceptionMessage(e));
                    return null;
                }
            }
        }

        final Throttle      throttle = makeThrottle(configValues);
        final InputStream   in = object.getPayload().openStream();
        final InputStream   wrappedstream = new InputStream()
        {
            @Override
            public void close() throws IOException
            {
                in.close();
            }

            @Override
            public int read() throws IOException
            {
                throttle.throttle(1);
                return in.read();
            }

            @Override
            public int read(byte[] b) throws IOException
            {
                int bytesRead = in.read(b);
                if ( bytesRead > 0 )
                {
                    throttle.throttle(bytesRead);
                }
                return bytesRead;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException
            {
                int bytesRead = in.read(b, off, len);
                if ( bytesRead > 0 )
                {
                    throttle.throttle(bytesRead);
                }
                return bytesRead;
            }
        };

        return new BackupStream()
        {
            @Override
            public InputStream getStream()
            {
                return wrappedstream;
            }

            @Override
            public void close() throws IOException
            {
                in.close();
            }
        };
    }

    @Override
    public void downloadBackup(Exhibitor exhibitor, BackupMetaData backup, OutputStream destination, Map<String, String> configValues) throws Exception
    {
    	System.out.println("downloadBackup:"+backup);

        byte[]          buffer = new byte[MIN_SWIFT_PART_SIZE];

        long            startMs = System.currentTimeMillis();
        RetryPolicy     retryPolicy = makeRetryPolicy(configValues);
        int             retryCount = 0;
        boolean         done = false;

        while ( !done )
        {
            Throttle            throttle = makeThrottle(configValues);
            InputStream         in = null;
            try
            {
                ObjectApi	    objectApi = getObjectApi(configValues.get(CONFIG_CONTAINER.getKey()));
                SwiftObject     object = objectApi.get(toKey(backup, configValues));
                in = object.getPayload().openStream();

                for(;;)
                {
                    int     bytesRead = in.read(buffer);
                    if ( bytesRead < 0 )
                    {
                        break;
                    }

                    throttle.throttle(bytesRead);
                    destination.write(buffer, 0, bytesRead);
                }

                done = true;
            }
            catch ( Exception e )
            {
            	e.printStackTrace();
                if ( !retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
                {
                    done = true;
                }
            }
            finally
            {
                CloseableUtils.closeQuietly(in);
            }
        }
    }
 

    @Override
    public List<BackupMetaData> getAvailableBackups(Exhibitor exhibitor, Map<String, String> configValues) throws Exception
    {
    	System.out.println("getAvailableBackups:"+getKeyPrefix(configValues));

        String            keyPrefix = getKeyPrefix(configValues);
        List<BackupMetaData>    completeList = Lists.newArrayList();

        ObjectApi	    objectApi = getObjectApi(configValues.get(CONFIG_CONTAINER.getKey()));

	    ObjectList list = objectApi.list();
	    for (int i=0; i< list.size(); i++)
	    {
	    	SwiftObject obj = list.get(i);
	    	String name = obj.getName();
	    	if (name.startsWith(keyPrefix))
	    		completeList.add(new BackupMetaData(name, obj.getLastModified().getTime()));

	    }
        return completeList;
    }

    @Override
    public void deleteBackup(Exhibitor exhibitor, BackupMetaData backup, Map<String, String> configValues) throws Exception
    {
        ObjectApi	objectApi = getObjectApi(configValues.get(CONFIG_CONTAINER.getKey()));
        objectApi.delete(toKey(backup, configValues));
    }

    private Throttle makeThrottle(final Map<String, String> configValues)
    {
        return new Throttle(this.getClass().getCanonicalName(), new Throttle.ThroughputFunction()
        {
            public int targetThroughput()
            {
                return Math.max(asInt(configValues.get(CONFIG_THROTTLE.getKey())), Integer.MAX_VALUE);
            }
        });
    }

    private ExponentialBackoffRetry makeRetryPolicy(Map<String, String> configValues)
    {
        return new ExponentialBackoffRetry(asInt(configValues.get(CONFIG_RETRY_SLEEP_MS.getKey())), asInt(configValues.get(CONFIG_MAX_RETRIES.getKey())));
    }


    private String toKey(BackupMetaData backup, Map<String, String> configValues)
    {
        String  name = backup.getName().replace(SEPARATOR, SEPARATOR_REPLACEMENT);
        String  prefix = getKeyPrefix(configValues);

        return prefix + SEPARATOR + name + SEPARATOR + backup.getModifiedDate();
    }

    private String getKeyPrefix(Map<String, String> configValues)
    {
        String  prefix = configValues.get(CONFIG_KEY_PREFIX.getKey());
        if ( prefix != null )
        {
            prefix = prefix.replace(SEPARATOR, SEPARATOR_REPLACEMENT);
        }

        if ( (prefix == null) || (prefix.length() == 0))
        {
            prefix = "exhibitor-backup";
        }
        return prefix;
    }

    private static BackupMetaData fromKey(String key)
    {
        String[]        parts = key.split("\\" + SEPARATOR);
        if ( parts.length != 3 )
        {
            return null;
        }
        return new BackupMetaData(parts[1], Long.parseLong(parts[2]));
    }
}
