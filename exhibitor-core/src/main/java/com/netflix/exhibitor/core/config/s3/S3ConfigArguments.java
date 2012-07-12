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

public class S3ConfigArguments
{
    private final String        bucket;
    private final String        key;
    private final String        heartbeatKeyPrefix;
    private final S3ConfigAutoManageLockArguments lockArguments;

    public S3ConfigArguments(String bucket, String key, String heartbeatKeyPrefix, S3ConfigAutoManageLockArguments lockArguments)
    {
        this.bucket = bucket;
        this.key = key;
        this.heartbeatKeyPrefix = heartbeatKeyPrefix;
        this.lockArguments = lockArguments;
    }

    public String getBucket()
    {
        return bucket;
    }

    public String getKey()
    {
        return key;
    }

    public String getHeartbeatKeyPrefix()
    {
        return heartbeatKeyPrefix;
    }

    public S3ConfigAutoManageLockArguments getLockArguments()
    {
        return lockArguments;
    }
}
