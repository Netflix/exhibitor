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
public class NewIndexRequest
{
    private String type;
    private String value;
    private NameAndModifiedDate backup;

    public NewIndexRequest()
    {
        this("", "", null);
    }

    public NewIndexRequest(String type, String value, NameAndModifiedDate backup)
    {
        this.type = type;
        this.value = value;
        this.backup = backup;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String value)
    {
        this.value = value;
    }

    public NameAndModifiedDate getBackup()
    {
        return backup;
    }

    public void setBackup(NameAndModifiedDate backup)
    {
        this.backup = backup;
    }
}
