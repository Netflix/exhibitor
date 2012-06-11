package com.netflix.exhibitor.core.backup.s3;

import com.amazonaws.services.s3.model.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.netflix.exhibitor.core.s3.S3Client;
import com.netflix.exhibitor.core.s3.S3Credential;
import com.netflix.exhibitor.core.s3.S3Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class MockS3Client implements S3Client
{
    private final List<byte[]>              uploadedBytes = new CopyOnWriteArrayList<byte[]>();
    private final ObjectListing             listing;
    private final Map<String, S3Object>     uploads = Maps.newConcurrentMap();

    private static final String BYTES_HEADER = "__internal_index__";

    public MockS3Client()
    {
        this(null, null);
    }

    public MockS3Client(S3Object object, ObjectListing listing)
    {
        if ( object != null )
        {
            S3Object        value = new S3Object();
            value.setKey(object.getKey());
            value.setObjectMetadata(object.getObjectMetadata());
            value.setObjectContent(object.getObjectContent());
            uploads.put(object.getKey(), value);
        }
        this.listing = listing;
    }

    @Override
    public void changeCredentials(S3Credential credential) throws Exception
    {
        // NOP
    }

    @Override
    public void close() throws IOException
    {
        // NOP
    }

    @Override
    public synchronized PutObjectResult putObject(PutObjectRequest request) throws Exception
    {
        Map<String, String>     userData = Maps.newHashMap();
        userData.put(BYTES_HEADER, Integer.toString(uploadedBytes.size()));

        ByteArrayOutputStream   out = new ByteArrayOutputStream();
        ByteStreams.copy(request.getInputStream(), out);
        byte[]                  bytes = out.toByteArray();
        uploadedBytes.add(bytes);

        byte[]              md5bytes = S3Utils.md5(bytes, out.size());

        S3Object            object = new S3Object();
        object.setKey(request.getKey());
        ObjectMetadata      metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        metadata.setUserMetadata(userData);
        object.setObjectMetadata(metadata);
        uploads.put(request.getKey(), object);

        PutObjectResult     result = new PutObjectResult();
        result.setETag(S3Utils.toHex(md5bytes));
        return result;
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request) throws Exception
    {
        return new InitiateMultipartUploadResult();
    }

    @Override
    public synchronized S3Object getObject(String bucket, String key) throws Exception
    {
        S3Object            s3Object = uploads.get(key);
        if ( s3Object != null )
        {
            S3Object        copy = new S3Object();
            copy.setKey(key);
            copy.setObjectMetadata(s3Object.getObjectMetadata());

            String              bytesIndexStr = s3Object.getObjectMetadata().getUserMetadata().get(BYTES_HEADER);
            if ( bytesIndexStr != null  )
            {
                S3ObjectInputStream     objectContent = new S3ObjectInputStream(new ByteArrayInputStream(uploadedBytes.get(Integer.parseInt(bytesIndexStr))), null);
                copy.setObjectContent(objectContent);
            }

            return copy;
        }
        return s3Object;
    }

    @Override
    public synchronized ObjectListing listObjects(ListObjectsRequest request) throws Exception
    {
        if ( listing != null )
        {
            return listing;
        }

        ObjectListing      localListing = new ObjectListing();
        for ( String key : uploads.keySet() )
        {
            boolean addIt = false;
            if ( request.getPrefix() != null )
            {
                if ( key.startsWith(request.getPrefix()) )
                {
                    addIt = true;
                }
            }

            if ( addIt )
            {
                S3ObjectSummary     summary = new S3ObjectSummary();
                summary.setKey(key);
                localListing.getObjectSummaries().add(summary);
            }
        }
        return localListing;
    }

    @Override
    public ObjectListing listNextBatchOfObjects(ObjectListing previousObjectListing) throws Exception
    {
        ObjectListing listing = new ObjectListing();
        listing.setTruncated(false);
        return listing;
    }

    @Override
    public synchronized void deleteObject(String bucket, String key) throws Exception
    {
        uploads.remove(key);
    }

    @Override
    public synchronized UploadPartResult uploadPart(UploadPartRequest request) throws Exception
    {
        ByteArrayOutputStream       out = new ByteArrayOutputStream();
        ByteStreams.copy(request.getInputStream(), out);

        uploadedBytes.add(out.toByteArray());

        byte[]              md5bytes = S3Utils.md5(out.toByteArray(), out.size());

        UploadPartResult    result = new UploadPartResult();
        result.setPartNumber(request.getPartNumber());
        result.setETag(S3Utils.toHex(md5bytes));
        return result;
    }

    @Override
    public void completeMultipartUpload(CompleteMultipartUploadRequest request) throws Exception
    {
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request) throws Exception
    {
    }

    public List<byte[]> getUploadedBytes()
    {
        return Lists.newArrayList(uploadedBytes);
    }
}
