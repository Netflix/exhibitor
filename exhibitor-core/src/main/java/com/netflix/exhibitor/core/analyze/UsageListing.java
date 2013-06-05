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

import com.google.common.collect.Maps;
import org.apache.curator.utils.ZKPaths;
import com.netflix.exhibitor.core.Exhibitor;
import org.apache.zookeeper.data.Stat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class UsageListing
{
    private final Exhibitor                 exhibitor;
    private final String                    startPath;
    private final int                       maxChildren;
    private final Map<String, NodeEntry>    details = Maps.newTreeMap();

    public static class NodeEntry
    {
        private final NodeEntry parent;
        private final int       directChildQty;
        private final long      creationDate;
        private int             deepChildQty;

        private NodeEntry(NodeEntry parent, int directChildQty, long creationDate)
        {
            this.parent = parent;
            this.directChildQty = directChildQty;
            this.deepChildQty = 0;
            this.creationDate = creationDate;
        }

        private void addToDeepCount(int count)
        {
            deepChildQty += count;
            if ( parent != null )
            {
                parent.addToDeepCount(count);
            }
        }

        public int getDirectChildQty()
        {
            return directChildQty;
        }

        public long getCreationDate()
        {
            return creationDate;
        }

        public int getDeepChildQty()
        {
            return deepChildQty;
        }
    }

    public UsageListing(Exhibitor exhibitor, String startPath, int maxChildren)
    {
        if ( startPath.trim().length() == 0 )
        {
            startPath = "/";
        }
        ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(startPath);
        this.exhibitor = exhibitor;
        this.startPath = ZKPaths.makePath(pathAndNode.getPath(), pathAndNode.getNode());
        this.maxChildren = maxChildren;
    }

    public void         generate() throws Exception
    {
        processNode(startPath, null);
    }

    public Iterator<String> getPaths()
    {
        return details.keySet().iterator();
    }

    public NodeEntry        getNodeDetails(String path)
    {
        return details.get(path);
    }

    private void        processNode(String path, NodeEntry parent) throws Exception
    {
        Stat        stat = exhibitor.getLocalConnection().checkExists().forPath(path);
        if ( stat == null )
        {
            return; // probably got deleted
        }

        NodeEntry       entry = new NodeEntry(parent, stat.getNumChildren(), stat.getCtime());
        details.put(path, entry);

        entry.addToDeepCount(stat.getNumChildren());
        if ( stat.getNumChildren() <= maxChildren )
        {
            List<String> children = exhibitor.getLocalConnection().getChildren().forPath(path);
            for ( String child : children )
            {
                String  thisPath = ZKPaths.makePath(path, child);
                processNode(thisPath, entry);
            }
        }
    }
}
