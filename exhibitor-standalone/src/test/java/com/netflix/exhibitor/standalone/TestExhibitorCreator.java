/*
 *
 *  Copyright 2016 Netflix, Inc.
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

package com.netflix.exhibitor.standalone;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.netflix.exhibitor.core.s3.S3CredentialsProvider;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNotNull;

public class TestExhibitorCreator {

    @Test
    public void makeCredentialsProviderTest() throws Exception{

        S3CredentialsProvider provider = ExhibitorCreator.makeCredentialsProvider("com.netflix.exhibitor.standalone.TestS3CredsProvider");
        assertNotNull(provider);
    }
}

class TestS3CredsProvider implements S3CredentialsProvider {

    @Override
    public AWSCredentialsProvider getAWSCredentialProvider() {
        return new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new BasicAWSCredentials("access","secret");
            }

            @Override
            public void refresh() {

            }
        };
    }
}