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

package com.netflix.exhibitor.core.backup.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.netflix.curator.RetryLoop;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.backup.BackupConfigSpec;
import com.netflix.exhibitor.core.backup.BackupMetaData;
import com.netflix.exhibitor.core.backup.BackupProvider;
import com.netflix.exhibitor.core.backup.BackupStream;
import com.netflix.exhibitor.core.s3.S3Client;
import com.netflix.exhibitor.core.s3.S3ClientFactory;
import com.netflix.exhibitor.core.s3.S3Credential;
import com.netflix.exhibitor.core.s3.S3Utils;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.netflix.exhibitor.core.config.DefaultProperties.asInt;

public class S3BackupProvider implements BackupProvider
{
    private final S3Client s3Client;

    private static final BackupConfigSpec CONFIG_THROTTLE = new BackupConfigSpec("throttle", "Throttle (bytes/ms)", "Data throttling. Maximum bytes per millisecond.", Integer.toString(1024 * 1024), BackupConfigSpec.Type.INTEGER);
    private static final BackupConfigSpec CONFIG_BUCKET = new BackupConfigSpec("bucket-name", "S3 Bucket Name", "The S3 bucket to use", "", BackupConfigSpec.Type.STRING);
    private static final BackupConfigSpec CONFIG_KEY_PREFIX = new BackupConfigSpec("key-prefix", "S3 Key Prefix", "The prefix for S3 backup keys", "exhibitor-backup", BackupConfigSpec.Type.STRING);
    private static final BackupConfigSpec CONFIG_MAX_RETRIES = new BackupConfigSpec("max-retries", "Max Retries", "Maximum retries when uploading/downloading S3 data", "3", BackupConfigSpec.Type.INTEGER);
    private static final BackupConfigSpec CONFIG_RETRY_SLEEP_MS = new BackupConfigSpec("retry-sleep-ms", "Retry Sleep (ms)", "Sleep time in milliseconds when retrying", "1000", BackupConfigSpec.Type.INTEGER);

    private static final List<BackupConfigSpec>     CONFIGS = Arrays.asList(CONFIG_THROTTLE, CONFIG_BUCKET, CONFIG_KEY_PREFIX, CONFIG_MAX_RETRIES, CONFIG_RETRY_SLEEP_MS);
    
    private static final int        MIN_S3_PART_SIZE = 5 * (1024 * 1024);

    @VisibleForTesting
    static final String       SEPARATOR = "/";
    private static final String       SEPARATOR_REPLACEMENT = "_";

    public S3BackupProvider(S3ClientFactory factory, S3Credential credential) throws Exception
    {
        s3Client = factory.makeNewClient(credential);
    }

    public S3Client getS3Client()
    {
        return s3Client;
    }

    @Override
    public List<BackupConfigSpec> getConfigs()
    {
        return CONFIGS;
    }

    @Override
    public boolean isValidConfig(Exhibitor exhibitor, Map<String, String> configValues)
    {
        String bucket = (configValues != null) ? configValues.get(CONFIG_BUCKET.getKey()) : null;
        return (bucket != null) && (bucket.trim().length() > 0);
    }

    @Override
    public UploadResult uploadBackup(Exhibitor exhibitor, BackupMetaData backup, File source, final Map<String, String> configValues) throws Exception
    {
        List<BackupMetaData>    availableBackups = getAvailableBackups(exhibitor, configValues);
        if ( availableBackups.contains(backup) )
        {
            return UploadResult.DUPLICATE;
        }

        RetryPolicy retryPolicy = makeRetryPolicy(configValues);
        Throttle    throttle = makeThrottle(configValues);

        String                          key = toKey(backup, configValues);

        if ( source.length() < MIN_S3_PART_SIZE )
        {
            byte[]          bytes = Files.toByteArray(source);
            S3Utils.simpleUploadFile(s3Client, bytes, configValues.get(CONFIG_BUCKET.getKey()), key);
        }
        else
        {
            multiPartUpload(source, configValues, retryPolicy, throttle, key);
        }

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

    private void multiPartUpload(File source, Map<String, String> configValues, RetryPolicy retryPolicy, Throttle throttle, String key) throws Exception
    {
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(configValues.get(CONFIG_BUCKET.getKey()), key);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

        byte[]      buffer = new byte[MIN_S3_PART_SIZE];
        InputStream in = null;
        try
        {
            List<PartETag>      eTags = Lists.newArrayList();
            int                 index = 1;

            in = new FileInputStream(source);
            for(;;)
            {
                int     bytesRead = in.read(buffer);
                if ( bytesRead < 0 )
                {
                    break;
                }
                throttle.throttle(bytesRead);

                PartETag eTag = uploadChunkWithRetry(buffer, bytesRead, initResponse, index++, retryPolicy);
                eTags.add(eTag);
            }

            completeUpload(initResponse, eTags);
        }
        catch ( Exception e )
        {
            abortUpload(initResponse);
            throw e;
        }
        finally
        {
            Closeables.closeQuietly(in);
        }
    }

    @Override
    public BackupStream getBackupStream(Exhibitor exhibitor, BackupMetaData backup, Map<String, String> configValues) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        RetryPolicy     retryPolicy = makeRetryPolicy(configValues);
        S3Object        object = null;
        int             retryCount = 0;
        while ( object == null )
        {
            try
            {
                object = s3Client.getObject(configValues.get(CONFIG_BUCKET.getKey()), toKey(backup, configValues));
            }
            catch ( AmazonS3Exception e)
            {
                if ( e.getErrorType() == AmazonServiceException.ErrorType.Client )
                {
                    return null;
                }

                if ( !retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
                {
                    return null;
                }
            }
        }

        final Throttle      throttle = makeThrottle(configValues);
        final InputStream   in = object.getObjectContent();
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
        byte[]          buffer = new byte[MIN_S3_PART_SIZE];

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
                S3Object            object = s3Client.getObject(configValues.get(CONFIG_BUCKET.getKey()), toKey(backup, configValues));
                in = object.getObjectContent();

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
                if ( !retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
                {
                    done = true;
                }
            }
            finally
            {
                Closeables.closeQuietly(in);
            }
        }
    }

    @Override
    public List<BackupMetaData> getAvailableBackups(Exhibitor exhibitor, Map<String, String> configValues) throws Exception
    {
        String            keyPrefix = getKeyPrefix(configValues);

        ListObjectsRequest      request = new ListObjectsRequest();
        request.setBucketName(configValues.get(CONFIG_BUCKET.getKey()));
        request.setPrefix(keyPrefix);

        List<BackupMetaData>    completeList = Lists.newArrayList();

        ObjectListing           listing = null;
        do
        {
            listing = (listing == null) ? s3Client.listObjects(request) : s3Client.listNextBatchOfObjects(listing);

            Iterable<S3ObjectSummary> filtered = Iterables.filter
            (
                listing.getObjectSummaries(),
                new Predicate<S3ObjectSummary>()
                {
                    @Override
                    public boolean apply(S3ObjectSummary summary)
                    {
                        return fromKey(summary.getKey()) != null;
                    }
                }
            );

            Iterable<BackupMetaData> transformed = Iterables.transform
            (
                filtered,
                new Function<S3ObjectSummary, BackupMetaData>()
                {
                    @Override
                    public BackupMetaData apply(S3ObjectSummary summary)
                    {
                        return fromKey(summary.getKey());
                    }
                }
            );

            completeList.addAll(Lists.newArrayList(transformed));
        } while ( listing.isTruncated() );
        return completeList;
    }

    @Override
    public void deleteBackup(Exhibitor exhibitor, BackupMetaData backup, Map<String, String> configValues) throws Exception
    {
        s3Client.deleteObject(configValues.get(CONFIG_BUCKET.getKey()), toKey(backup, configValues));
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


    private PartETag uploadChunkWithRetry(byte[] buffer, int bytesRead, InitiateMultipartUploadResult initResponse, int index, RetryPolicy retryPolicy) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        int             retries = 0;
        for(;;)
        {
            try
            {
                return uploadChunk(buffer, bytesRead, initResponse, index);
            }
            catch ( Exception e )
            {
                if ( !retryPolicy.allowRetry(retries++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
                {
                    throw e;
                }
            }
        }
    }

    private PartETag uploadChunk(byte[] buffer, int bytesRead, InitiateMultipartUploadResult initResponse, int index) throws Exception
    {
        byte[]          md5 = S3Utils.md5(buffer, bytesRead);

        UploadPartRequest   request = new UploadPartRequest();
        request.setBucketName(initResponse.getBucketName());
        request.setKey(initResponse.getKey());
        request.setUploadId(initResponse.getUploadId());
        request.setPartNumber(index);
        request.setPartSize(bytesRead);
        request.setMd5Digest(S3Utils.toBase64(md5));
        request.setInputStream(new ByteArrayInputStream(buffer, 0, bytesRead));

        UploadPartResult    response = s3Client.uploadPart(request);
        PartETag            partETag = response.getPartETag();
        if ( !response.getPartETag().getETag().equals(S3Utils.toHex(md5)) )
        {
            throw new Exception("Unable to match MD5 for part " + index);
        }

        return partETag;
    }

    private void completeUpload(InitiateMultipartUploadResult initResponse, List<PartETag> eTags) throws Exception
    {
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(initResponse.getBucketName(), initResponse.getKey(), initResponse.getUploadId(), eTags);
        s3Client.completeMultipartUpload(completeRequest);
    }

    private void abortUpload(InitiateMultipartUploadResult initResponse) throws Exception
    {
        AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(initResponse.getBucketName(), initResponse.getKey(), initResponse.getUploadId());
        s3Client.abortMultipartUpload(abortRequest);
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
