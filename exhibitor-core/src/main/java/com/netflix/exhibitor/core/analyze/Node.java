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

import com.google.common.collect.Lists;
import java.util.List;

class Node
{
    private final String        value;
    private final NodeTypes     type;
    private final List<Node>    edges;

    // guarded by sync
    // use an int instead of a boolean so that it doesn't need to be cleared for each iteration
    private int flagValue;

    Node(NodeTypes type, String value)
    {
        this.type = type;
        this.value = value;
        edges = Lists.newArrayList();
        flagValue = 0;
    }

    NodeTypes getType()
    {
        return type;
    }

    String getValue()
    {
        return value;
    }

    List<Node> getEdges()
    {
        return edges;
    }

    synchronized int getFlagValue()
    {
        return flagValue;
    }

    synchronized void setFlagValue(int newValue)
    {
        flagValue = newValue;
    }

    @Override
    public String toString()
    {
        return value;
    }
}
