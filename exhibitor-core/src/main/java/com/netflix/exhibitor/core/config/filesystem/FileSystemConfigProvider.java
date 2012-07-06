/*
 *
 *  Copyright 2011 Netflix, Inc.
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

package com.netflix.exhibitor.core.config.filesystem;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.netflix.exhibitor.core.config.AutoManageLockArguments;
import com.netflix.exhibitor.core.config.ConfigCollection;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.PseudoLock;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.channels.FileLock;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Config provider that uses a properties file store locally
 */
public class FileSystemConfigProvider implements ConfigProvider
{
    private final File propertiesDirectory;
    private final String propertyFileName;
    private final String heartbeatFilePrefix;
    private final Properties defaults;
    private final AutoManageLockArguments autoManageLockArguments;
    private final AtomicLong lastHeartbeatCleanup = new AtomicLong(0);

    private static final String             FILE_CONTENT = "foo";

    private static final int                CLEANUP_PERIOD_MS = (int)TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
    private static final int                MAX_AGE_MS = (int)TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

    /**
     * @param propertiesDirectory where to store the properties
     * @param propertyFileName name of the file to store properties in
     * @param heartbeatFilePrefix prefix for heartbeat files
     * @param autoManageLockArguments config arguments for auto managing instances
     * @throws IOException if directory cannot be created
    */
    public FileSystemConfigProvider(File propertiesDirectory, String propertyFileName, String heartbeatFilePrefix, AutoManageLockArguments autoManageLockArguments) throws IOException
    {
        this(propertiesDirectory, propertyFileName, heartbeatFilePrefix, new Properties(), autoManageLockArguments);
    }

    /**
     * @param propertiesDirectory where to store the properties
     * @param propertyFileName name of the file to store properties in
     * @param heartbeatFilePrefix prefix for heartbeat files
     * @param defaults default values  @throws IOException if directory cannot be created
     * @param autoManageLockArguments config arguments for auto managing instances
     * @throws IOException if directory cannot be created
     */
    public FileSystemConfigProvider(File propertiesDirectory, String propertyFileName, String heartbeatFilePrefix, Properties defaults, AutoManageLockArguments autoManageLockArguments) throws IOException
    {
        this.propertiesDirectory = propertiesDirectory;
        this.propertyFileName = propertyFileName;
        this.heartbeatFilePrefix = heartbeatFilePrefix;
        this.defaults = defaults;
        this.autoManageLockArguments = autoManageLockArguments;

        if ( propertiesDirectory.exists() && !propertiesDirectory.isDirectory() )
        {
            throw new IllegalArgumentException("The file argument should be a directory: " + propertiesDirectory);
        }

        if ( !propertiesDirectory.exists() && !propertiesDirectory.mkdirs() )
        {
            throw new IOException("Could not create directory: " + propertiesDirectory);
        }
    }

    @Override
    public PseudoLock newPseudoLock() throws Exception
    {
        return new FileSystemPseudoLock(propertiesDirectory, autoManageLockArguments.getPrefix(), autoManageLockArguments.getTimeoutMs(), autoManageLockArguments.getPollingMs());
    }

    @Override
    public LoadedInstanceConfig loadConfig() throws Exception
    {
        File            propertiesFile = new File(propertiesDirectory, propertyFileName);
        Properties      properties = new Properties();
        if ( propertiesFile.exists() )
        {
            RandomAccessFile    raf = new RandomAccessFile(propertiesFile, "rw");
            try
            {
                FileLock        lock = raf.getChannel().lock();
                try
                {
                    properties.load(Channels.newInputStream(raf.getChannel()));
                }
                finally
                {
                    lock.release();
                }
            }
            finally
            {
                Closeables.closeQuietly(raf);
            }
        }
        PropertyBasedInstanceConfig config = new PropertyBasedInstanceConfig(properties, defaults);
        return new LoadedInstanceConfig(config, propertiesFile.lastModified());
    }

    @Override
    public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareLastModified) throws Exception
    {
        File                            propertiesFile = new File(propertiesDirectory, propertyFileName);
        PropertyBasedInstanceConfig     propertyBasedInstanceConfig = new PropertyBasedInstanceConfig(config);

        long                lastModified = 0;
        FileOutputStream    fileStream = new FileOutputStream(propertiesFile);
        OutputStream        out = new BufferedOutputStream(fileStream);
        try
        {
            FileLock lock = fileStream.getChannel().lock();
            try
            {
                propertyBasedInstanceConfig.getProperties().store(out, "Auto-generated by Exhibitor");
                lastModified = propertiesFile.lastModified();
            }
            finally
            {
                lock.release();
            }
        }
        finally
        {
            Closeables.closeQuietly(out);
        }

        return new LoadedInstanceConfig(propertyBasedInstanceConfig, lastModified);
    }

    @Override
    public void writeInstanceHeartbeat(String instanceHostname) throws Exception
    {
        File file = getHeartbeatFile(instanceHostname);
        Files.write(FILE_CONTENT.getBytes(), file);
    }

    private File getHeartbeatFile(String instanceHostname) throws UnsupportedEncodingException
    {
        String      fixedHostname = URLEncoder.encode(instanceHostname, "UTF-8");
        return new File(propertiesDirectory, heartbeatFilePrefix + fixedHostname);
    }

    @Override
    public long getLastHeartbeatForInstance(String instanceHostname) throws Exception
    {
        long        lastCleanupMs = lastHeartbeatCleanup.get();
        if ( (System.currentTimeMillis() - lastCleanupMs) >= CLEANUP_PERIOD_MS )
        {
            if ( lastHeartbeatCleanup.compareAndSet(lastCleanupMs, System.currentTimeMillis()) )
            {
                doCleanup();
            }
        }

        File file = getHeartbeatFile(instanceHostname);
        return file.exists() ? file.lastModified() : 0;
    }

    private void doCleanup()
    {
        File[]      files = propertiesDirectory.listFiles();
        if ( files != null )
        {
            for ( File f : files )
            {
                if ( f.getName().startsWith(heartbeatFilePrefix) )
                {
                    long        age = System.currentTimeMillis() - f.lastModified();
                    if ( age > MAX_AGE_MS )
                    {
                        //noinspection ResultOfMethodCallIgnored
                        f.delete();
                    }
                }
            }
        }
    }
}
