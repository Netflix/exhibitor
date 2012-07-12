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
public class UsageListingRequest
{
    private int         maxChildrenForTraversal;
    private String      startPath;

    public UsageListingRequest()
    {
        this("/", Integer.MAX_VALUE);
    }

    public UsageListingRequest(String startPath, int maxChildrenForTraversal)
    {
        this.maxChildrenForTraversal = maxChildrenForTraversal;
        this.startPath = startPath;
    }

    public int getMaxChildrenForTraversal()
    {
        return maxChildrenForTraversal;
    }

    public void setMaxChildrenForTraversal(int maxChildrenForTraversal)
    {
        this.maxChildrenForTraversal = maxChildrenForTraversal;
    }

    public String getStartPath()
    {
        return startPath;
    }

    public void setStartPath(String startPath)
    {
        this.startPath = startPath;
    }
}
