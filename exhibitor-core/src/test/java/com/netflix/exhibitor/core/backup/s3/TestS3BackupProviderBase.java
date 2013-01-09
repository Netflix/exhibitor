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

import com.amazonaws.services.s3.model.*;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.netflix.exhibitor.core.backup.BackupMetaData;
import com.netflix.exhibitor.core.s3.PropertyBasedS3Credential;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class TestS3BackupProviderBase
{
    protected final File        sourceFile;

    protected TestS3BackupProviderBase(File sourceFile)
    {
        this.sourceFile = sourceFile;
    }

    @Test
    public void   testUpload() throws Exception
    {
        byte[]                    uploadedBytes = getUploadedBytes(sourceFile);

        byte[] fileBytes = Files.toByteArray(sourceFile);
        Assert.assertEquals(uploadedBytes, fileBytes);
    }

    @Test
    public void     testDownload() throws Exception
    {
        InputStream in = null;
        OutputStream        out = null;
        File                tempFile = File.createTempFile("test", ".test");
        try
        {
            in = new FileInputStream(sourceFile);

            PutObjectRequest dummyRequest =
                    new PutObjectRequest("bucket", "exhibitor-backup" + S3BackupProvider.SEPARATOR + "test" + S3BackupProvider.SEPARATOR + 1, in, null);

            MockS3Client        s3Client = new MockS3Client(null, null);
            s3Client.putObject(dummyRequest);

            S3BackupProvider    provider = new S3BackupProvider(new MockS3ClientFactory(s3Client), new PropertyBasedS3Credential(new Properties()), null);
            out = new FileOutputStream(tempFile);
            provider.downloadBackup(null, new BackupMetaData("test", 1), out, Maps.<String, String>newHashMap());
            
            Assert.assertEquals(Files.toByteArray(sourceFile), Files.toByteArray(tempFile));
        }
        finally
        {
            Closeables.closeQuietly(in);
            Closeables.closeQuietly(out);
            
            //noinspection ResultOfMethodCallIgnored
            tempFile.delete();
        }
    }

    @Test
    public void testGetAvailableBackupKeys() throws Exception
    {
        ObjectListing       listing = new ObjectListing()
        {
            @Override
            public List<S3ObjectSummary> getObjectSummaries()
            {
                List<S3ObjectSummary>       list = Lists.newArrayList();
                S3ObjectSummary             summary = new S3ObjectSummary();
                summary.setKey("exhibitor-backup" + S3BackupProvider.SEPARATOR + "one" + S3BackupProvider.SEPARATOR + "1234");
                list.add(summary);
                summary = new S3ObjectSummary();
                summary.setKey("exhibitor-backup" + S3BackupProvider.SEPARATOR + "two" + S3BackupProvider.SEPARATOR + "1234");
                list.add(summary);
                summary = new S3ObjectSummary();
                summary.setKey("exhibitor-backup" + S3BackupProvider.SEPARATOR + "three" + S3BackupProvider.SEPARATOR + "1234");
                list.add(summary);
                return list;
            }
        };

        MockS3Client            s3Client = new MockS3Client(null, listing);
        S3BackupProvider        provider = new S3BackupProvider(new MockS3ClientFactory(s3Client), new PropertyBasedS3Credential(new Properties()), null);
        List<BackupMetaData>    backups = provider.getAvailableBackups(null, Maps.<String, String>newHashMap());
        List<String>            backupNames = Lists.transform
        (
            backups,
            new Function<BackupMetaData, String>()
            {
                @Override
                public String apply(BackupMetaData metaData)
                {
                    return metaData.getName();
                }
            }
        );
        Assert.assertEquals(backupNames, Arrays.asList("one", "two", "three"));
    }

    private byte[] getUploadedBytes(File sourceFile) throws Exception
    {
        MockS3Client        s3Client = new MockS3Client();
        S3BackupProvider    provider = new S3BackupProvider(new MockS3ClientFactory(s3Client), new PropertyBasedS3Credential(new Properties()), null);

        provider.uploadBackup(null, new BackupMetaData("test", 10), sourceFile, Maps.<String, String>newHashMap());

        ByteArrayOutputStream   out = new ByteArrayOutputStream();
        for ( byte[] bytes : s3Client.getUploadedBytes() )
        {
            out.write(bytes);
        }
        return out.toByteArray();
    }
}
