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

package com.netflix.exhibitor.core.s3;

import com.amazonaws.services.s3.model.*;
import java.io.Closeable;

public interface S3Client extends Closeable
{
    public void     changeCredentials(S3Credential credential) throws Exception;

    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws Exception;

    public S3Object getObject(String bucket, String key) throws Exception;

    public ObjectMetadata getObjectMetadata(String bucket, String key) throws Exception;

    public ObjectListing listObjects(ListObjectsRequest request) throws Exception;

    public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) throws Exception;

    public PutObjectResult putObject(PutObjectRequest request) throws Exception;

    public void deleteObject(String bucket, String key) throws Exception;

    public UploadPartResult uploadPart(UploadPartRequest request) throws Exception;

    public void completeMultipartUpload(CompleteMultipartUploadRequest request) throws Exception;

    public void abortMultipartUpload(AbortMultipartUploadRequest request) throws Exception;
}
