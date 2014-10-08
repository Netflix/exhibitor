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

package com.netflix.exhibitor.processes;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessMonitor implements Closeable
{
    private final ExecutorService   service = Executors.newCachedThreadPool();
    private final LoggingMinimum log;
    private final Map<ProcessTypes, ProcessHolder> processes = Maps.newConcurrentMap();

    public ProcessMonitor(LoggingMinimum log)
    {
        this.log = log;
    }

    @Override
    public void close() throws IOException
    {
        service.shutdownNow();
    }

    public enum Mode
    {
        LEAVE_RUNNING_ON_INTERRUPT,
        DESTROY_ON_INTERRUPT
    }

    public enum Streams
    {
        ERROR,
        STANDARD,
        BOTH
    }

    private static class ProcessHolder
    {
        final Process           process;
        final AtomicBoolean     isBeingClosed = new AtomicBoolean(false);

        private ProcessHolder(Process process)
        {
            this.process = process;
        }
    }

    public void     destroy(ProcessTypes type)
    {
        ProcessHolder   previousHolder = processes.remove(type);
        closeHolder(previousHolder);
    }

    public void     monitor(ProcessTypes type, final Process process, final String completionMessage, final Mode mode, final Streams whichStreams)
    {
        ProcessHolder   newHolder = new ProcessHolder(process);
        ProcessHolder   previousHolder = processes.put(type, newHolder);
        closeHolder(previousHolder);

        switch ( whichStreams )
        {
            case ERROR:
            {
                service.submit(makeStreamProc(process.getErrorStream(), type.getDescription(), LoggingMinimum.Level.ERROR, newHolder));
                break;
            }

            case STANDARD:
            {
                service.submit(makeStreamProc(process.getInputStream(), type.getDescription(), LoggingMinimum.Level.INFO, newHolder));
                break;
            }

            case BOTH:
            {
                service.submit(makeStreamProc(process.getErrorStream(), type.getDescription(), LoggingMinimum.Level.ERROR, newHolder));
                service.submit(makeStreamProc(process.getInputStream(), type.getDescription(), LoggingMinimum.Level.INFO, newHolder));
                break;
            }
        }

        service.submit(makeEofProc(process, completionMessage, mode));
    }

    private void closeHolder(ProcessHolder previousHolder)
    {
        if ( previousHolder != null )
        {
            previousHolder.isBeingClosed.set(true);

            Closeables.closeQuietly(previousHolder.process.getErrorStream());
            Closeables.closeQuietly(previousHolder.process.getInputStream());
            Closeables.closeQuietly(previousHolder.process.getOutputStream());

            previousHolder.process.destroy();
        }
    }

    private Runnable makeEofProc(final Process process, final String completionMessage, final Mode mode)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    process.waitFor();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    if ( mode == Mode.DESTROY_ON_INTERRUPT )
                    {
                        Closeables.closeQuietly(process.getErrorStream());
                        Closeables.closeQuietly(process.getInputStream());
                        Closeables.closeQuietly(process.getOutputStream());

                        process.destroy();
                    }
                }

                if ( completionMessage != null )
                {
                    log.add(LoggingMinimum.Level.INFO, completionMessage);
                }
            }
        };
    }

    private Callable<Object> makeStreamProc(final InputStream stream, final String name, final LoggingMinimum.Level level, final ProcessHolder holder)
    {
        return new Callable<Object>()
        {
            public Object call() throws Exception
            {
                BufferedReader in = new BufferedReader(new InputStreamReader(stream));
                for (;;)
                {
                    String line = in.readLine();
                    if ( line == null )
                    {
                        break;
                    }
                    if ( !holder.isBeingClosed.get() )
                    {
                        log.add(level, name + ": " + line);
                    }
                }
                return null;
            }
        };
    }
}
