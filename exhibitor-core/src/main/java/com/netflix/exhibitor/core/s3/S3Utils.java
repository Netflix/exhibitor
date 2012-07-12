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

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.apache.commons.codec.binary.Base64;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.security.MessageDigest;
import java.util.Date;

public class S3Utils
{
    public static String toBase64(byte[] md5)
    {
        byte encoded[] = Base64.encodeBase64(md5, false);
        return new String(encoded);
    }

    public static byte[] md5(byte[] buffer, int length)
    {
        try
        {
            MessageDigest mdigest = MessageDigest.getInstance("MD5");
            mdigest.update(buffer, 0, length);
            return mdigest.digest();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String toHex(byte[] digest)
    {
        StringBuilder sb = new StringBuilder(digest.length * 2);
        for ( byte b : digest )
        {
            String hex = Integer.toHexString(b);
            if ( hex.length() == 1 )
            {
                sb.append("0");
            }
            else if ( hex.length() == 8 )
            {
                hex = hex.substring(6);
            }
            sb.append(hex);
        }
        return sb.toString().toLowerCase();
    }

    public static ObjectMetadata simpleUploadFile(S3Client client, byte[] bytes, String bucket, String key) throws Exception
    {
        byte[]                          md5 = md5(bytes, bytes.length);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        metadata.setLastModified(new Date());
        metadata.setContentMD5(S3Utils.toBase64(md5));
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, new ByteArrayInputStream(bytes), metadata);
        PutObjectResult putObjectResult = client.putObject(putObjectRequest);

        if ( !putObjectResult.getETag().equals(S3Utils.toHex(md5)) )
        {
            throw new Exception("Unable to match MD5 for config");
        }

        return metadata;
    }
}
