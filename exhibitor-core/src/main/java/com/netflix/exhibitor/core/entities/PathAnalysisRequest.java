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
public class PathAnalysisRequest
{
    private int         max;
    private String      path;

    public PathAnalysisRequest()
    {
        this(0, "");
    }

    public PathAnalysisRequest(int max, String path)
    {
        this.max = max;
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

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }
}
