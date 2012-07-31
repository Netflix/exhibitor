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

package com.netflix.exhibitor.core.config.filesystem;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
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
        if ( f.exists() && !f.delete() )
        {
            throw new IOException("Could not delete: " + f);
        }
    }

    @Override
    protected List<String> getFileNames(String lockPrefix) throws Exception
    {
        File[] files = directory.listFiles();
        if ( files != null )
        {
            Iterable<File> filtered = Iterables.filter
            (
                Arrays.asList(files),
                new Predicate<File>()
                {
                    @Override
                    public boolean apply(File f)
                    {
                        return f.getName().startsWith(getLockPrefix());
                    }
                }
            );

            Iterable<String> transformed = Iterables.transform
            (
                filtered,
                new Function<File, String>()
                {
                    @Override
                    public String apply(File f)
                    {
                        return f.getName();
                    }
                }
            );

            return Lists.newArrayList(transformed);
        }
        return Lists.newArrayList();
    }

    private File getFile(String key)
    {
        return new File(directory, key);
    }
}
