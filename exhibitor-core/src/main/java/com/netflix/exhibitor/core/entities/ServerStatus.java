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

import com.netflix.exhibitor.core.state.InstanceStateTypes;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ServerStatus
{
    private String      hostname;
    private int         code;
    private String      description;
    private boolean     isLeader;

    public ServerStatus()
    {
        this("", 0, "", false);
    }

    public ServerStatus(String hostname, int code, String description, boolean isLeader)
    {
        this.hostname = hostname;
        this.code = code;
        this.description = description;
        this.isLeader = isLeader;
    }

    public String getHostname()
    {
        return hostname;
    }

    public void setHostname(String hostname)
    {
        this.hostname = hostname;
    }

    public int getCode()
    {
        return code;
    }

    public void setCode(int code)
    {
        this.code = code;
    }

    public InstanceStateTypes getInstanceStateType()
    {
        return InstanceStateTypes.fromCode(code);
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public boolean getIsLeader()
    {
        return isLeader;
    }

    public void setIsLeader(boolean leader)
    {
        isLeader = leader;
    }

    @Override
    public String toString()
    {
        return "ServerStatus{" +
            "hostname='" + hostname + '\'' +
            ", code=" + code +
            ", description='" + description + '\'' +
            ", isLeader=" + isLeader +
            '}';
    }

    @SuppressWarnings("RedundantIfStatement")
    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ServerStatus that = (ServerStatus)o;

        if ( code != that.code )
        {
            return false;
        }
        if ( isLeader != that.isLeader )
        {
            return false;
        }
        if ( !description.equals(that.description) )
        {
            return false;
        }
        if ( !hostname.equals(that.hostname) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = hostname.hashCode();
        result = 31 * result + code;
        result = 31 * result + description.hashCode();
        result = 31 * result + (isLeader ? 1 : 0);
        return result;
    }
}
