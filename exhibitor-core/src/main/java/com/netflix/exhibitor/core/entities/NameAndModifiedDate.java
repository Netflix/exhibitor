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

@XmlRootElement
@SuppressWarnings("UnusedDeclaration")
public class NameAndModifiedDate
{
    private String  name;
    private long    modifiedDate;

    public NameAndModifiedDate()
    {
        this("", 0);
    }

    public NameAndModifiedDate(String name, long modifiedDate)
    {
        this.name = name;
        this.modifiedDate = modifiedDate;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public long getModifiedDate()
    {
        return modifiedDate;
    }

    public void setModifiedDate(long modifiedDate)
    {
        this.modifiedDate = modifiedDate;
    }
}
