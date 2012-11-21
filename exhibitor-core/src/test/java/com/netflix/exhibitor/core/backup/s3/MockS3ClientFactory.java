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

import com.netflix.exhibitor.core.s3.S3Client;
import com.netflix.exhibitor.core.s3.S3ClientFactory;
import com.netflix.exhibitor.core.s3.S3Credential;
import com.netflix.exhibitor.core.s3.S3CredentialsProvider;

public class MockS3ClientFactory implements S3ClientFactory
{
    private final MockS3Client s3Client;

    public MockS3ClientFactory(MockS3Client s3Client)
    {
        this.s3Client = s3Client;
    }

    @Override
    public S3Client makeNewClient(S3Credential credentials) throws Exception
    {
        return s3Client;
    }

    @Override
    public S3Client makeNewClient(S3CredentialsProvider credentialsProvider) throws Exception {
        return s3Client;
    }
}
