package com.netflix.exhibitor.core.azure;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class AzureClientImpl implements AzureClient {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final String storageConnectionString;

    public AzureClientImpl(AzureCredential credentials) {
        this.storageConnectionString =
            "DefaultEndpointsProtocol=http" +
            ";AccountName=" + credentials.getAccountName() +
            ";AccountKey=" + credentials.getAccountKey();
    }

    @Override
    public CloudBlobClient getClient() throws Exception {
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(this.storageConnectionString);
        return storageAccount.createCloudBlobClient();
    }

    @Override
    public CloudBlob getBlob(String containerName, String uri) throws Exception {
        CloudBlobContainer container = getClient().getContainerReference(containerName);
        return container.getBlockBlobReference(uri);
    }

    @Override
    public BlobProperties getBlobProperties(String containerName, String uri) throws Exception {
        CloudBlob blob = getBlob(containerName, uri);
        blob.downloadAttributes();
        return blob.getProperties();
    }

    @Override
    public Iterable<ListBlobItem> listBlobs(String containerName, String prefix) throws Exception {
        CloudBlobContainer container = getClient().getContainerReference(containerName);
        return container.listBlobs(prefix);
    }

    @Override
    public void putBlob(byte[] bytes, String containerName, String uri) throws Exception {
        CloudBlob blob = getBlob(containerName, uri);
        blob.getContainer().createIfNotExists();
        blob.upload(new ByteArrayInputStream(bytes), bytes.length);
    }

    @Override
    public void deleteBlob(String containerName, String uri) throws Exception {
        getBlob(containerName, uri).deleteIfExists();
    }

    @Override
    public void close() throws IOException {

    }
}
