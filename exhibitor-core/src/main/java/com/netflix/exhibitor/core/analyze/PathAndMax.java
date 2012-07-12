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

import com.google.common.base.Preconditions;

public class PathAndMax
{
    private final String path;
    private final int max;

    /**
     * @param path path for the lock. Either the parent path or one of the lock ZNodes
     * @param maxLocks max locks for this path. i.e. a mutex has a max of 1, a semaphore has a max
     *                 of 1 - n.
     */
    public PathAndMax(String path, int maxLocks)
    {
        this.path = Preconditions.checkNotNull(path, "path cannot be null");
        max = maxLocks;
    }

    public String getPath()
    {
        return path;
    }

    public int getMax()
    {
        return max;
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

        PathAndMax that = (PathAndMax)o;

        if ( !path.equals(that.path) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return path.hashCode();
    }

    @Override
    public String toString()
    {
        return path + "(" + max + ")";
    }
}
