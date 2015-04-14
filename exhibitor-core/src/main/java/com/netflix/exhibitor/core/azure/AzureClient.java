package com.netflix.exhibitor.core.azure;

import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.ListBlobItem;

import java.io.Closeable;

public interface AzureClient extends Closeable {
    public CloudBlobClient getClient() throws Exception;

    public CloudBlob getBlob(String containerName, String uri) throws Exception;

    public BlobProperties getBlobProperties(String containerName, String uri) throws Exception;

    public Iterable<ListBlobItem> listBlobs(String containerName, String prefix) throws Exception;

    public void putBlob(byte[] bytes, String containerName, String uri) throws Exception;

    public void deleteBlob(String containerName, String uri) throws Exception;

}
