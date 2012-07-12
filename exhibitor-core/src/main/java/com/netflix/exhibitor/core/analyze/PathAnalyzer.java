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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.curator.utils.ZKPaths;
import com.netflix.exhibitor.core.Exhibitor;
import org.apache.zookeeper.KeeperException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class PathAnalyzer
{
    private final Exhibitor exhibitor;
    private final List<PathAndMax> paths;

    private static final Comparator<String> childComparator = new Comparator<String>()
    {
        @Override
        public int compare(String p1, String p2)
        {
            String[]        parts1 = p1.split("-");
            String[]        parts2 = p2.split("-");

            String sequence1 = parts1[parts1.length - 1];
            String sequence2 = parts2[parts2.length - 1];

            return sequence1.compareTo(sequence2);
        }
    };

    /**
     * @param exhibitor instance
     * @param paths list of paths to check and the max locks for each path (i.e. InterProcessMutex
     *              has a max of 1, InterProcessSemaphore has a max of 1-n). The path can point
     *              at either the parent or one of the lock ZNodes. The analyzer figures out which
     *              is which by seeing if the node has children or not
     * @throws Exception
     */
    public PathAnalyzer(Exhibitor exhibitor, List<PathAndMax> paths) throws Exception
    {
        this.exhibitor = exhibitor;
        this.paths = ImmutableList.copyOf(paths);
    }

    /**
     * Perform the analysis and return the results. Always check {@link Analysis#getError()}
     * to see if there was an error generating the results.
     *
     * @return analysis
     * @throws Exception unrecoverable errors
     */
    public Analysis     analyze() throws Exception
    {
        AtomicReference<String>     error = new AtomicReference<String>(null);
        Map<String, PathComplete>   loadedPaths = loadedPaths(paths, error);
        if ( error.get() != null )
        {
            return new Analysis(error.get(), Lists.<PathComplete>newArrayList(), Lists.<Set<String>>newArrayList());
        }

        List<Node>        resourceAllocationGraph = buildResourceAllocationGraph(loadedPaths);
        List<Set<String>> possibleCycles = checkForCycles(resourceAllocationGraph, loadedPaths);

        return new Analysis(null, loadedPaths.values(), possibleCycles);
    }

    @VisibleForTesting
    protected List<String> getChildren(String path) throws Exception
    {
        return exhibitor.getLocalConnection().getChildren().forPath(path);
    }

    @VisibleForTesting
    protected String getId(String fullPath) throws Exception
    {
        byte[] bytes = exhibitor.getLocalConnection().getData().forPath(fullPath);
        return new String(bytes);
    }

    private Map<String, PathComplete> loadedPaths(List<PathAndMax> paths, AtomicReference<String> error) throws Exception
    {
        ImmutableMap.Builder<String, PathComplete> builder = ImmutableMap.builder();
        for ( PathAndMax pathAndMax : paths )
        {
            String          thisPath = pathAndMax.getPath();
            List<String>    children;
            try
            {
                children = getChildren(thisPath);
                if ( children.size() == 0 )
                {
                    thisPath = ZKPaths.getPathAndNode(thisPath).getPath();    // assume a child node was passed - use its parent
                    if ( thisPath.equals("/") )
                    {
                        continue;   // don't do the root
                    }
                    children = getChildren(thisPath);
                }
            }
            catch ( KeeperException.NoNodeException dummy )
            {
                error.set("Path not found: " + thisPath);
                break;
            }
            Collections.sort(children, childComparator);

            List<String>    childIds;
            try
            {
                childIds = Lists.newArrayList();
                for ( String childName : children )
                {
                    String  fullPath = ZKPaths.makePath(thisPath, childName);
                    childIds.add(getId(fullPath));
                }
            }
            catch ( KeeperException.NoNodeException dummy )
            {
                error.set("Path not found: " + thisPath);
                break;
            }
            builder.put(thisPath, new PathComplete(thisPath, pathAndMax.getMax(), childIds));
        }

        return builder.build();
    }

    private List<Set<String>> checkForCycles(List<Node> resourceAllocationGraph, Map<String, PathComplete> loadedPaths)
    {
        List<Set<String>>   possibleCycles = Lists.newArrayList();
        int                 flagValue = 1;
        for ( Node n : resourceAllocationGraph )
        {
            Set<String>     possibleCyclePaths = Sets.newHashSet();
            if ( hasPossibleCycle(n, flagValue++, possibleCyclePaths, loadedPaths.get(n.getValue()).getMax()) )
            {
                possibleCycles.add(possibleCyclePaths);
            }
        }

        Set<Set<String>>        deduper = Sets.newHashSet();
        Iterator<Set<String>>   iterator = possibleCycles.iterator();
        while ( iterator.hasNext() )
        {
            Set<String> s = iterator.next();
            if ( deduper.contains(s) )
            {
                iterator.remove();
            }
            else
            {
                deduper.add(s);
            }
        }

        return possibleCycles;
    }

    private boolean hasPossibleCycle(Node n, int flagValue, Set<String> possibleCyclePaths, int max)
    {
        if ( n.getType() == NodeTypes.PATH )
        {
            possibleCyclePaths.add(getDisplayStr(n));
        }

        if ( n.getFlagValue() == flagValue )
        {
            return true;
        }
        else
        {
            n.setFlagValue(flagValue);

            int         lockCount = 0;
            for ( Node child : n.getEdges() )
            {
                if ( lockCount++ < max )
                {
                    possibleCyclePaths.add(getDisplayStr(child));
                }
                if ( hasPossibleCycle(child, flagValue, possibleCyclePaths, max) )
                {
                    return true;
                }
            }
        }
        return false;
    }

    private String getDisplayStr(Node n)
    {
        return n.getType() + ":" + n.getValue();
    }

    private List<Node> buildResourceAllocationGraph(Map<String, PathComplete> loadedPaths) throws Exception
    {
        List<Node>          pathNodes = Lists.newArrayList();
        Map<String, Node>   processNodes = Maps.newHashMap();
        for ( PathComplete pathComplete : loadedPaths.values() )
        {
            Node                node = new Node(NodeTypes.PATH, pathComplete.getPath());
            pathNodes.add(node);

            if ( pathComplete.getChildQty() > 0 )
            {
                int         lockCount = 0;
                for ( String childId : pathComplete.getChildIds() )
                {
                    Node    processNode = getProcessNode(childId, processNodes);
                    if ( lockCount++ < pathComplete.getMax() )
                    {
                        processNode.getEdges().add(node);
                    }
                    else
                    {
                        node.getEdges().add(processNode);
                    }
                }
            }
        }

        return pathNodes;
    }

    private Node getProcessNode(String childId, Map<String, Node> processNodes) throws Exception
    {
        Node        node = processNodes.get(childId);
        if ( node == null )
        {
            node = new Node(NodeTypes.PROCESS, childId);
            processNodes.put(childId, node);
        }
        return node;
    }
}
