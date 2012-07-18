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

import com.google.common.io.Closeables;
import org.apache.jute.Record;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.Version;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IndexBuilder implements Closeable
{
    private final File              directory;
    private final AtomicInteger     count = new AtomicInteger(0);
    private final AtomicLong        from = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong        to = new AtomicLong(Long.MIN_VALUE);

    private NIOFSDirectory niofsDirectory;
    private IndexWriter writer;

    public IndexBuilder(File directory)
    {
        this.directory = directory;
    }

    public void open() throws Exception
    {
        if ( !directory.exists() && !directory.mkdirs() )
        {
            throw new IOException("Could not make: " + directory);
        }

        IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new KeywordAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        niofsDirectory = new NIOFSDirectory(directory, new SingleInstanceLockFactory());
        writer = new IndexWriter(niofsDirectory, conf);
    }

    public void add(InputStream stream) throws Exception
    {
        ZooKeeperLogParser  logParser = new ZooKeeperLogParser(stream);
        if ( logParser.isValid() )
        {
            logParser.parse
            (
                new LogEntryReceiver()
                {
                    @Override
                    public void receiveEntry(TxnHeader header, Record record) throws Exception
                    {
                        indexRecord(header, record, count, from, to);
                    }
                }
            );
        }
    }

    public void writeMetaData() throws Exception
    {
        IndexMetaData       metaData = new IndexMetaData(new Date(from.get()), new Date(to.get()), count.get());
        IndexMetaData.write(metaData, IndexMetaData.getMetaDataFile(directory));
    }

    @Override
    public void close() throws IOException
    {
        Closeables.closeQuietly(writer);
        Closeables.closeQuietly(niofsDirectory);
    }

    public int  getCurrentCount()
    {
        return count.get();
    }

    private void indexRecord(TxnHeader header, Record record, AtomicInteger count, AtomicLong from, AtomicLong to) throws IOException
    {
        if ( record instanceof CreateTxn )
        {
            CreateTxn   createTxn = (CreateTxn)record;

            EntryTypes type = createTxn.getEphemeral() ? EntryTypes.CREATE_EPHEMERAL: EntryTypes.CREATE_PERSISTENT;
            Document document = makeDocument(header, type, count, from, to);
            addPath(document, createTxn.getPath());
            addData(document, createTxn.getData());
            writer.addDocument(document);
        }
        else if ( record instanceof DeleteTxn )
        {
            DeleteTxn   deleteTxn = (DeleteTxn)record;

            Document document = makeDocument(header, EntryTypes.DELETE, count, from, to);
            addPath(document, deleteTxn.getPath());
            writer.addDocument(document);
        }
        else if ( record instanceof SetDataTxn )
        {
            SetDataTxn   setDataTxn = (SetDataTxn)record;

            NumericField versionField = new NumericField(FieldNames.VERSION, Field.Store.YES, true);
            versionField.setIntValue(setDataTxn.getVersion());

            Document document = makeDocument(header, EntryTypes.SET_DATA, count, from, to);
            addPath(document, setDataTxn.getPath());
            addData(document, setDataTxn.getData());
            document.add(versionField);
        }
    }

    private void addData(Document document, byte[] data)
    {
        document.add(new Field(FieldNames.DATA, data));
    }

    private void addPath(Document document, String path)
    {
        document.add(new Field(FieldNames.PATH, path, Field.Store.YES, Field.Index.NOT_ANALYZED));
    }

    private Document makeDocument(TxnHeader header, EntryTypes type, AtomicInteger count, AtomicLong from, AtomicLong to)
    {
        count.incrementAndGet();
        if ( header.getTime() < from.get() )
        {
            from.set(header.getTime());
        }
        if ( header.getTime() > to.get() )
        {
            to.set(header.getTime());
        }

        NumericField dateField = new NumericField(FieldNames.DATE, Field.Store.YES, true);
        dateField.setLongValue(header.getTime());

        Document    document = new Document();
        document.add(new Field(FieldNames.TYPE, Integer.toString(type.getId()), Field.Store.YES, Field.Index.NOT_ANALYZED));
        document.add(dateField);
        return document;
    }
}
