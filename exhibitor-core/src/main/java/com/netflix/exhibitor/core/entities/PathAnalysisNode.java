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

import com.google.common.collect.Lists;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public class PathAnalysisNode
{
    private String          path;
    private int             max;
    private List<String>    childIds;

    public PathAnalysisNode()
    {
        this("", 1, Lists.<String>newArrayList());
    }

    public PathAnalysisNode(String path, int max, List<String> childIds)
    {
        this.path = path;
        this.max = max;
        this.childIds = childIds;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public int getMax()
    {
        return max;
    }

    public void setMax(int max)
    {
        this.max = max;
    }

    public List<String> getChildIds()
    {
        return childIds;
    }

    public void setChildIds(List<String> childIds)
    {
        this.childIds = childIds;
    }
}
