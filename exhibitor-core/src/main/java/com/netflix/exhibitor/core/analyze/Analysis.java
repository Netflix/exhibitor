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
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class Analysis
{
    private final String                    error;
    private final Collection<PathComplete>  completeData;
    private final List<Set<String>>         possibleCycles;

    Analysis(String error, Collection<PathComplete> completeData, List<Set<String>> possibleCycles)
    {
        this.error = error;
        this.completeData = ImmutableList.copyOf(completeData);

        ImmutableList.Builder<Set<String>> builder = ImmutableList.builder();
        for ( Set<String> pc : possibleCycles )
        {
            builder.add(ImmutableSet.copyOf(pc));
        }
        this.possibleCycles = builder.build();
    }

    public String getError()
    {
        return error;
    }

    public List<Set<String>> getPossibleCycles()
    {
        return possibleCycles;
    }

    public Collection<PathComplete> getCompleteData()
    {
        return completeData;
    }
}
