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

import org.apache.lucene.search.TopDocs;

public class CachedSearch
{
    private final TopDocs docs;
    private final int totalDocs;

    CachedSearch(TopDocs docs, int totalDocs)
    {
        this.docs = docs;
        this.totalDocs = totalDocs;
    }

    public int  getTotalHits()
    {
        return (docs != null) ? docs.totalHits : totalDocs;
    }
    
    public int  getNthDocId(int n)
    {
        return (docs != null) ? docs.scoreDocs[n].doc : n;
    }
}
