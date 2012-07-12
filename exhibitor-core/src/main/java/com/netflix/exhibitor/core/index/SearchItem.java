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

import java.util.Date;

public class SearchItem
{
    private final int           type;
    private final String        path;
    private final int           version;
    private final Date          date;

    public SearchItem(int type, String path, int version, Date date)
    {
        this.type = type;
        this.path = path;
        this.version = version;
        this.date = date;
    }

    public int getType()
    {
        return type;
    }

    public String getPath()
    {
        return path;
    }

    public int getVersion()
    {
        return version;
    }

    public Date getDate()
    {
        return date;
    }

    @Override
    public String toString()
    {
        return "SearchItem{" +
            "type=" + type +
            ", path='" + path + '\'' +
            ", version=" + version +
            ", date=" + date +
            '}';
    }
}
