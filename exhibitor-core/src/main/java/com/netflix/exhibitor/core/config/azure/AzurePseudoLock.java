package com.netflix.exhibitor.core.config.azure;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.netflix.exhibitor.core.azure.AzureClient;
import com.netflix.exhibitor.core.config.PseudoLockBase;

import java.util.ArrayList;
import java.util.List;

public class AzurePseudoLock extends PseudoLockBase
{
    private final AzureClient      client;
    private final String        container;

    /**
     * @param client the Azure client
     * @param container the Azure container
     * @param lockPrefix the Azure blob prefix
     * @param timeoutMs max age for locks
     * @param pollingMs how often to poll Azure
     * @param settlingMs how long to wait for Azure to reach consistency
     */
    public AzurePseudoLock(AzureClient client, String container, String lockPrefix, int timeoutMs, int pollingMs, int settlingMs)
    {
        super(lockPrefix, timeoutMs, pollingMs, settlingMs);
        this.client = client;
        this.container = container;
    }

    @Override
    protected void createFile(String uri, byte[] contents) throws Exception
    {
        client.putBlob(contents, container, uri);
    }

    @Override
    protected void deleteFile(String uri) throws Exception
    {
        try
        {
            client.deleteBlob(container, uri);
        }
        catch ( StorageException ignore )
        {
            // ignore these
        }
    }

    @Override
    protected List<String> getFileNames(String lockPrefix) throws Exception {
        Iterable<ListBlobItem> blobs = client.listBlobs(container, lockPrefix);

        List<String> fileNames = new ArrayList<String>();
        for (ListBlobItem blob : blobs) {
            fileNames.add(((CloudBlockBlob) blob).getName());
        }

        return fileNames;
    }
}
