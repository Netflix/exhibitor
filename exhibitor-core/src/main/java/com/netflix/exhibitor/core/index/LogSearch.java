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

package com.netflix.exhibitor.core.index;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.Closeables;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class LogSearch implements Closeable
{
    private final Directory directory;
    private final IndexReader reader;
    private final IndexSearcher searcher;
    private final File file;
    private final Cache<String, TopDocs> cache = CacheBuilder.newBuilder()
        .expireAfterAccess(5, TimeUnit.MINUTES) // does this need to be configurable?
        .build();

    private static final int            MAX_RESULTS = 5000; // does this need to be configurable?

    public LogSearch(File file) throws Exception
    {
        this.file = file;
        directory = new NIOFSDirectory(file, new NativeFSLockFactory());
        reader = IndexReader.open(directory);
        searcher = new IndexSearcher(reader);
    }

    public File getFile()
    {
        return file;
    }

    public int      getDocQty()
    {
        return reader.numDocs();
    }

    public void     releaseCache(String id)
    {
        cache.invalidate(id);
    }

    public String    cacheSearch(Query query, String reuseId, int maxResults) throws IOException
    {
        if ( (maxResults <= 0) || (maxResults > MAX_RESULTS) )
        {
            maxResults = MAX_RESULTS;
        }
        
        String      id = ((reuseId != null) && (reuseId.length() > 0)) ? reuseId : UUID.randomUUID().toString();
        if ( query != null )    // otherwise it's an All Docs search which is the default
        {
            TopDocs docs = search(query, maxResults);
            docs.totalHits = Math.min(maxResults, docs.totalHits);
            cache.put(id, docs);
        }
        return id;
    }
    
    public CachedSearch getCachedSearch(String id)
    {
        return new CachedSearch(cache.getIfPresent(id), getDocQty());
    }

    public TopDocs   search(Query query, int maxResults) throws IOException
    {
        Sort sort = new Sort(new SortField(FieldNames.DATE, SortField.LONG, true));
        return searcher.search(query, maxResults, sort);
    }

    public SearchItem toResult(int documentId) throws IOException
    {
        Document        document = searcher.doc(documentId);

        String          type = document.getFieldable(FieldNames.TYPE).stringValue();
        NumericField    date = (NumericField)document.getFieldable(FieldNames.DATE);
        Fieldable       path = document.getFieldable(FieldNames.PATH);
        NumericField    version = (NumericField)document.getFieldable(FieldNames.VERSION);
        return new SearchItem
        (
            Integer.parseInt(type),
            path.stringValue(),
            (version != null) ? version.getNumericValue().intValue() : -1,
            new Date(date.getNumericValue().longValue())
        );
    }

    public byte[]           toData(int documentId) throws IOException
    {
        Document document = searcher.doc(documentId);
        return document.getBinaryValue(FieldNames.DATA);
    }

    @Override
    public void close()
    {
        Closeables.closeQuietly(searcher);
        Closeables.closeQuietly(reader);
        Closeables.closeQuietly(directory);
    }
}
