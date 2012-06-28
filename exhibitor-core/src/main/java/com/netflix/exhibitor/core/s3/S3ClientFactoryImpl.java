/*
 *
 *  Copyright 2011 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.exhibitor.core.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class S3ClientFactoryImpl implements S3ClientFactory
{
    @Override
    public S3Client makeNewClient(final S3Credential credentials) throws Exception
    {
        return new S3Client()
        {
            private final AtomicReference<RefCountedClient>   client = new AtomicReference<RefCountedClient>(null);
            {
                changeCredentials(credentials);
            }

            @Override
            public void changeCredentials(S3Credential credential) throws Exception
            {
                RefCountedClient   newRefCountedClient = (credential != null) ? new RefCountedClient(new AmazonS3Client(new BasicAWSCredentials(credentials.getAccessKeyId(), credentials.getSecretAccessKey()))) : null;
                RefCountedClient   oldRefCountedClient = client.getAndSet(newRefCountedClient);
                if ( oldRefCountedClient != null )
                {
                    oldRefCountedClient.markForDelete();
                }
            }

            @Override
            public void close() throws IOException
            {
                try
                {
                    changeCredentials(null);
                }
                catch ( Exception e )
                {
                    throw new IOException(e);
                }
            }

            @Override
            public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws Exception
            {
                RefCountedClient holder = client.get();
                AmazonS3Client amazonS3Client = holder.useClient();
                try
                {
                    return amazonS3Client.initiateMultipartUpload(request);
                }
                finally
                {
                    holder.release();
                }
            }

            @Override
            public PutObjectResult putObject(PutObjectRequest request) throws Exception
            {
                RefCountedClient holder = client.get();
                AmazonS3Client amazonS3Client = holder.useClient();
                try
                {
                    return amazonS3Client.putObject(request);
                }
                finally
                {
                    holder.release();
                }
            }

            @Override
            public S3Object getObject(String bucket, String key) throws Exception
            {
                RefCountedClient holder = client.get();
                AmazonS3Client amazonS3Client = holder.useClient();
                try
                {
                    return amazonS3Client.getObject(bucket, key);
                }
                finally
                {
                    holder.release();
                }
            }

            @Override
            public ObjectMetadata getObjectMetadata(String bucket, String key) throws Exception
            {
                RefCountedClient holder = client.get();
                AmazonS3Client amazonS3Client = holder.useClient();
                try
                {
                    return amazonS3Client.getObjectMetadata(bucket, key);
                }
                finally
                {
                    holder.release();
                }
            }

            @Override
            public ObjectListing listObjects(ListObjectsRequest request) throws Exception
            {
                RefCountedClient    holder = client.get();
                AmazonS3Client      amazonS3Client = holder.useClient();
                try
                {
                    return amazonS3Client.listObjects(request);
                }
                finally
                {
                    holder.release();
                }
            }

            @Override
            public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) throws Exception
            {
                RefCountedClient holder = client.get();
                AmazonS3Client amazonS3Client = holder.useClient();
                try
                {
                    return amazonS3Client.listNextBatchOfObjects(previousObjectListing);
                }
                finally
                {
                    holder.release();
                }
            }

            @Override
            public void deleteObject(String bucket, String key) throws Exception
            {
                RefCountedClient holder = client.get();
                AmazonS3Client amazonS3Client = holder.useClient();
                try
                {
                    amazonS3Client.deleteObject(bucket, key);
                }
                finally
                {
                    holder.release();
                }
            }

            @Override
            public UploadPartResult uploadPart(UploadPartRequest request) throws Exception
            {
                RefCountedClient holder = client.get();
                AmazonS3Client amazonS3Client = holder.useClient();
                try
                {
                    return amazonS3Client.uploadPart(request);
                }
                finally
                {
                    holder.release();
                }
            }

            @Override
            public void completeMultipartUpload(CompleteMultipartUploadRequest request) throws Exception
            {
                RefCountedClient holder = client.get();
                AmazonS3Client amazonS3Client = holder.useClient();
                try
                {
                    amazonS3Client.completeMultipartUpload(request);
                }
                finally
                {
                    holder.release();
                }
            }

            @Override
            public void abortMultipartUpload(AbortMultipartUploadRequest request) throws Exception
            {
                RefCountedClient holder = client.get();
                AmazonS3Client amazonS3Client = holder.useClient();
                try
                {
                    amazonS3Client.abortMultipartUpload(request);
                }
                finally
                {
                    holder.release();
                }
            }
        };
    }
}
