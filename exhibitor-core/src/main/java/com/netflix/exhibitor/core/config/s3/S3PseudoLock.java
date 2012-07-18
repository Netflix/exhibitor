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
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.netflix.exhibitor.core.config.PseudoLockBase;
import com.netflix.exhibitor.core.s3.S3Client;
import java.io.ByteArrayInputStream;
import java.util.List;

public class S3PseudoLock extends PseudoLockBase
{
    private final S3Client      client;
    private final String        bucket;

    /**
     * @param client the S3 client
     * @param bucket the S3 bucket
     * @param lockPrefix key prefix
     * @param timeoutMs max age for locks
     * @param pollingMs how often to poll S3
     */
    public S3PseudoLock(S3Client client, String bucket, String lockPrefix, int timeoutMs, int pollingMs)
    {
        super(lockPrefix, timeoutMs, pollingMs);
        this.client = client;
        this.bucket = bucket;
    }

    /**
     * @param client the S3 client
     * @param bucket the S3 bucket
     * @param lockPrefix key prefix
     * @param timeoutMs max age for locks
     * @param pollingMs how often to poll S3
     * @param settlingMs how long to wait for S3 to reach consistency
     */
    public S3PseudoLock(S3Client client, String bucket, String lockPrefix, int timeoutMs, int pollingMs, int settlingMs)
    {
        super(lockPrefix, timeoutMs, pollingMs, settlingMs);
        this.client = client;
        this.bucket = bucket;
    }

    @Override
    protected void createFile(String key, byte[] contents) throws Exception
    {
        ObjectMetadata      metadata = new ObjectMetadata();
        metadata.setContentLength(contents.length);
        PutObjectRequest    request = new PutObjectRequest(bucket, key, new ByteArrayInputStream(contents), metadata);
        client.putObject(request);
    }

    @Override
    protected void deleteFile(String key) throws Exception
    {
        try
        {
            client.deleteObject(bucket, key);
        }
        catch ( AmazonServiceException ignore )
        {
            // ignore these
        }
    }

    @Override
    protected byte[] getFileContents(String key) throws Exception
    {
        S3Object    object = null;
        try
        {
            object = client.getObject(bucket, key);
        }
        catch ( AmazonServiceException e )
        {
            // ignore - treat it as missing
        }
        if ( object != null )
        {
            try
            {
                byte[]      bytes = new byte[(int)object.getObjectMetadata().getContentLength()];
                ByteStreams.read(object.getObjectContent(), bytes, 0, bytes.length);
                return bytes;
            }
            finally
            {
                Closeables.closeQuietly(object.getObjectContent());
            }
        }
        return null;
    }

    @Override
    protected List<String> getFileNames(String lockPrefix) throws Exception
    {
        ListObjectsRequest  request = new ListObjectsRequest();
        request.setBucketName(bucket);
        request.setPrefix(lockPrefix);
        ObjectListing objectListing = client.listObjects(request);

        return Lists.transform
        (
            objectListing.getObjectSummaries(),
            new Function<S3ObjectSummary, String>()
            {
                @Override
                public String apply(S3ObjectSummary summary)
                {
                    return summary.getKey();
                }
            }
        );
    }
}
