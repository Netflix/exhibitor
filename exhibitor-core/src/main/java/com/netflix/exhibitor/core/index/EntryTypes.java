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

public enum EntryTypes
{
    CREATE_PERSISTENT(0),
    CREATE_EPHEMERAL(1),
    DELETE(2),
    SET_DATA(3)
    ;

    private final int id;

    public int getId()
    {
        return id;
    }

    public static EntryTypes    getFromId(int id)
    {
        for ( EntryTypes type : values() )
        {
            if ( type.getId() == id )
            {
                return type;
            }
        }
        return null;
    }

    public static EntryTypes    getFromId(String id)
    {
        int     intId = 0;
        try
        {
            intId = Integer.parseInt(id);
        }
        catch ( NumberFormatException ignore )
        {
            // ignore
        }
        return getFromId(intId);
    }

    private EntryTypes(int id)
    {
        this.id = id;
    }
}
