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

package com.netflix.exhibitor.core.entities;

import javax.xml.bind.annotation.XmlRootElement;

@SuppressWarnings("UnusedDeclaration")
@XmlRootElement
public class Index
{
    private String  name;
    private String  from;
    private String  to;
    private int     entryCount;

    public Index()
    {
        this("", "", "", 0);
    }

    public Index(String name, String from, String to, int entryCount)
    {
        this.name = name;
        this.from = from;
        this.to = to;
        this.entryCount = entryCount;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getFrom()
    {
        return from;
    }

    public void setFrom(String from)
    {
        this.from = from;
    }

    public String getTo()
    {
        return to;
    }

    public void setTo(String to)
    {
        this.to = to;
    }

    public int getEntryCount()
    {
        return entryCount;
    }

    public void setEntryCount(int entryCount)
    {
        this.entryCount = entryCount;
    }
}
