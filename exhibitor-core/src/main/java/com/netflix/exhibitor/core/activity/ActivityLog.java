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

package com.netflix.exhibitor.core.activity;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ActivityLog
{
    private final Queue<Message>    queue = new ConcurrentLinkedQueue<Message>();
    private final int               windowSizeLines;

    private static final Logger     log = LoggerFactory.getLogger(ActivityLog.class);

    private static class Message
    {
        final Date      date = new Date();
        final String    text;
        final Type      type;

        private Message(String text, Type type)
        {
            this.text = text;
            this.type = type;
        }
    }

    /**
     * @param windowSizeLines max lines to keep in memory
     */
    public ActivityLog(int windowSizeLines)
    {
        this.windowSizeLines = windowSizeLines;
        add(Type.INFO, "Exhibitor started");
    }

    /**
     * Return the current window lines
     *
     * @param separator line separator
     * @return lines
     */
    public List<String> toDisplayList(final String separator)
    {
        Iterable<String> transformed = Iterables.transform
        (
            queue,
            new Function<Message, String>()
            {
                public String apply(Message message)
                {
                    return message.date + separator + message.type + separator + message.text;
                }
            }
        );
        return Lists.reverse(ImmutableList.copyOf(transformed));
    }

    /**
     * Supported line types
     */
    public enum Type
    {
        ERROR()
        {
            @Override
            protected void log(String message, Throwable exception)
            {
                if ( exception != null )
                {
                    log.error(message, exception);
                }
                else
                {
                    log.error(message);
                }
            }

            @Override
            protected boolean addToUI()
            {
                return true;
            }
        },

        INFO()
        {
            @Override
            protected void log(String message, Throwable exception)
            {
                if ( exception != null )
                {
                    log.info(message, exception);
                }
                else
                {
                    log.info(message);
                }
            }

            @Override
            protected boolean addToUI()
            {
                return true;
            }
        },

        DEBUG()
        {
            @Override
            protected void log(String message, Throwable exception)
            {
                if ( exception != null )
                {
                    log.debug(message, exception);
                }
                else
                {
                    log.debug(message);
                }
            }

            @Override
            protected boolean addToUI()
            {
                return Boolean.getBoolean("log-debug");
            }
        }

        ;

        protected abstract void  log(String message, Throwable exception);

        protected abstract boolean  addToUI();
    }

    /**
     * Add a log message
     *
     * @param type message type
     * @param message the messqage
     */
    public void         add(Type type, String message)
    {
        add(type, message, null);
    }

    /**
     * Add a log message with an exception
     *
     * @param type message type
     * @param message the messqage
     * @param exception the exception
     */
    public void         add(Type type, String message, Throwable exception)
    {
        String          queueMessage = message;
        if ( (type == Type.ERROR) && (exception != null) )
        {
            String  exceptionMessage = exception.getMessage();
            if ( exceptionMessage == null )
            {
                exceptionMessage = getExceptionMessage(exception);
            }
            queueMessage += " (" + exceptionMessage + ")";
        }

        if ( type.addToUI() )
        {
            while ( queue.size() > windowSizeLines )  // NOTE: due to concurrency, this may make the queue shorter than MAX - that's OK (and in some cases longer)
            {
                queue.remove();
            }
            queue.add(new Message(queueMessage, type));
        }
        type.log(message, exception);
    }

    private String getExceptionMessage(Throwable exception)
    {
        StringWriter        out = new StringWriter();
        exception.printStackTrace(new PrintWriter(out));
        BufferedReader      in = new BufferedReader(new StringReader(out.toString()));
        try
        {
            StringBuilder       str = new StringBuilder();
            for(;;)
            {
                String  line = in.readLine();
                if ( line == null )
                {
                    break;
                }
                if ( str.length() > 0 )
                {
                    str.append(", ");
                }
                str.append(line);
            }
            return str.toString();
        }
        catch ( IOException e )
        {
            // ignore
        }
        return "n/a";
    }
}
