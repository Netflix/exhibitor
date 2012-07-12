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
import java.util.Date;

@XmlRootElement
@SuppressWarnings("UnusedDeclaration")
public class SearchRequest
{
    private String      indexName;
    private String      pathPrefix;
    private Date        firstDate;
    private Date        secondDate;
    private int         operationType;
    private String      reuseHandle;
    private int         maxResults;

    public SearchRequest()
    {
        this("", "", null, null, -1, null, 0);
    }

    public SearchRequest(String indexName, String pathPrefix, Date firstDate, Date secondDate, int operationType, String reuseHandle, int maxResults)
    {
        this.indexName = indexName;
        this.pathPrefix = pathPrefix;
        this.firstDate = firstDate;
        this.secondDate = secondDate;
        this.operationType = operationType;
        this.reuseHandle = reuseHandle;
        this.maxResults = maxResults;
    }

    public int getMaxResults()
    {
        return maxResults;
    }

    public void setMaxResults(int maxResults)
    {
        this.maxResults = maxResults;
    }

    public String getReuseHandle()
    {
        return reuseHandle;
    }

    public void setReuseHandle(String reuseHandle)
    {
        this.reuseHandle = reuseHandle;
    }

    public String getIndexName()
    {
        return indexName;
    }

    public void setIndexName(String indexName)
    {
        this.indexName = indexName;
    }

    public String getPathPrefix()
    {
        return pathPrefix;
    }

    public void setPathPrefix(String pathPrefix)
    {
        this.pathPrefix = pathPrefix;
    }

    public Date getFirstDate()
    {
        return firstDate;
    }

    public void setFirstDate(Date firstDate)
    {
        this.firstDate = firstDate;
    }

    public Date getSecondDate()
    {
        return secondDate;
    }

    public void setSecondDate(Date secondDate)
    {
        this.secondDate = secondDate;
    }

    public int getOperationType()
    {
        return operationType;
    }

    public void setOperationType(int operationType)
    {
        this.operationType = operationType;
    }
}
