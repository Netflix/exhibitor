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

package com.netflix.exhibitor.core.analyze;

import com.google.common.collect.ImmutableList;
import java.util.List;

public class PathComplete
{
    private final String        path;
    private final int           max;
    private final List<String>  childIds;

    PathComplete(String path, int max, List<String> childIds)
    {
        this.path = path;
        this.max = max;
        this.childIds = ImmutableList.copyOf(childIds);
    }

    /**
     * @return the path
     */
    public String getPath()
    {
        return path;
    }

    /**
     * @return the max locks for the path
     */
    public int getMax()
    {
        return max;
    }

    /**
     * The process IDs of the path. The first {@link #getMax()} IDs
     * are deemed to own the lock
     *
     * @return process IDs
     */
    public List<String> getChildIds()
    {
        return childIds;
    }

    /**
     * @return number of children in the path
     */
    public int getChildQty()
    {
        return childIds.size();
    }

    @Override
    public String toString()
    {
        return "PathComplete{" +
            "path='" + path + '\'' +
            ", max=" + max +
            ", childIds=" + childIds +
            '}';
    }
}
