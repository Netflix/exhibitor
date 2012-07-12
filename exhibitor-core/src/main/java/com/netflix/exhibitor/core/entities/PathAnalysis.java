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
public class PathAnalysis
{
    private String                  error;
    private List<PathAnalysisNode>  nodes;
    private List<IdList>            possibleCycles;

    public PathAnalysis()
    {
        this(null, Lists.<PathAnalysisNode>newArrayList(), Lists.<IdList>newArrayList());
    }

    public PathAnalysis(String error, List<PathAnalysisNode> nodes, List<IdList> possibleCycles)
    {
        this.error = error;
        this.nodes = nodes;
        this.possibleCycles = possibleCycles;
    }

    public String getError()
    {
        return error;
    }

    public void setError(String error)
    {
        this.error = error;
    }

    public List<PathAnalysisNode> getNodes()
    {
        return nodes;
    }

    public void setNodes(List<PathAnalysisNode> nodes)
    {
        this.nodes = nodes;
    }

    public List<IdList> getPossibleCycles()
    {
        return possibleCycles;
    }

    public void setPossibleCycles(List<IdList> possibleCycles)
    {
        this.possibleCycles = possibleCycles;
    }
}
