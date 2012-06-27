package com.netflix.exhibitor.core.config.filesystem;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.netflix.exhibitor.core.config.PseudoLockBase;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FileSystemPseudoLock extends PseudoLockBase
{
    private final File directory;

    public FileSystemPseudoLock(File directory, String prefix, int timeoutMs, int pollingMs)
    {
        super(prefix, timeoutMs, pollingMs);
        this.directory = directory;
    }

    public FileSystemPseudoLock(File directory, String prefix, int timeoutMs, int pollingMs, int settlingMs)
    {
        super(prefix, timeoutMs, pollingMs, settlingMs);
        this.directory = directory;
    }

    @Override
    protected void createFile(String key, byte[] contents) throws Exception
    {
        Files.write(contents, getFile(key));
    }

    @Override
    protected void deleteFile(String key) throws Exception
    {
        File f = getFile(key);
        if ( !f.delete() )
        {
            throw new IOException("Could not delete: " + f);
        }
    }

    @Override
    protected byte[] getFileContents(String key) throws Exception
    {
        return Files.toByteArray(getFile(key));
    }

    @Override
    protected List<String> getFileNames(String lockPrefix) throws Exception
    {
        File[] files = directory.listFiles();
        if ( files != null )
        {
            return Lists.transform
            (
                Arrays.asList(files),
                new Function<File, String>()
                {
                    @Override
                    public String apply(File f)
                    {
                        return f.getName();
                    }
                }
            );
        }
        return Lists.newArrayList();
    }

    private File getFile(String key)
    {
        return new File(directory, key);
    }
}
