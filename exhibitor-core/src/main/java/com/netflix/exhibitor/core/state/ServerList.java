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

package com.netflix.exhibitor.core.state;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class ServerList
{
    private final List<ServerSpec>      specs;

    public ServerList(List<ServerSpec> specs)
    {
        this.specs = ImmutableList.copyOf(specs);
    }

    public ServerList(String serverSpec)
    {
        ImmutableList.Builder<ServerSpec> builder = ImmutableList.builder();

        String[]        items = serverSpec.split(",");
        for ( String item : items )
        {
            String  trimmed = item.trim();
            if ( trimmed.length() > 0 )
            {
                String[]    parts = trimmed.split(":");
                String      code = getPart(parts, 0);
                String      id = getPart(parts, 1);
                String      hostname = getPart(parts, 2);
                if ( (code != null) && (id != null) && (hostname != null) )
                {
                    try
                    {
                        int serverId = Integer.parseInt(id);
                        builder.add(new ServerSpec(hostname, serverId, ServerType.fromCode(code)));
                    }
                    catch ( NumberFormatException ignore )
                    {
                        // ignore
                    }
                }
            }
        }

        specs = builder.build();
    }

    private String getPart(String[] parts, int logicalIndex)
    {
        if ( parts.length == 3 )
        {
            return parts[logicalIndex].trim();
        }
        else if ( parts.length == 2 )
        {
            if ( logicalIndex == 0 )
            {
                return ServerType.STANDARD.getCode();
            }
            return parts[logicalIndex - 1].trim();
        }
        return null;
    }

    public String toSpecString()
    {
        StringBuilder       str = new StringBuilder();
        for ( ServerSpec spec : specs )
        {
            if ( str.length() > 0 )
            {
                str.append(",");
            }
            str.append(spec.getServerId()).append(":").append(spec.getHostname());
        }

        return str.toString();
    }

    public ServerSpec getSpec(final String hostname)
    {
        return Iterables.find
        (
            specs,
            new Predicate<ServerSpec>()
            {
                @Override
                public boolean apply(ServerSpec spec)
                {
                    return spec.getHostname().equals(hostname);
                }
            },
            null
        );
    }

    public List<ServerSpec> getSpecs()
    {
        return specs;
    }
    
    public Collection<String>   getHostnames()
    {
        return Lists.transform
        (
            specs,
            new Function<ServerSpec, String>()
            {
                @Override
                public String apply(ServerSpec spec)
                {
                    return spec.getHostname();
                }
            }
        );
    }

    public static Predicate<ServerSpec>  isUs(final String hostname)
    {
        return new Predicate<ServerSpec>()
        {
            @Override
            public boolean apply(ServerSpec spec)
            {
                return spec.getHostname().equals(hostname);
            }
        };
    }

    @Override
    public String toString()
    {
        return toSpecString();
    }

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

        ServerList that = (ServerList)o;

        Set<ServerSpec>     thisSpecs = Sets.newHashSet(specs);
        Set<ServerSpec>     thatSpecs = Sets.newHashSet(that.specs);
        //noinspection RedundantIfStatement
        if ( !thisSpecs.equals(thatSpecs) ) // we don't care about ordering - just that the lists have the same elements
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return specs.hashCode();
    }
}
