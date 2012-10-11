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
import com.google.common.collect.Maps;
import com.netflix.exhibitor.core.Exhibitor;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestPathAnalyzer
{
    @Test
    public void         testBasic() throws Exception
    {
        final Map<PathAndMax, List<String>>       data = Maps.newHashMap();
        data.put(new PathAndMax("/r1", 1), Arrays.asList("p1-0001", "p2-0002"));
        data.put(new PathAndMax("/r2", 1), Arrays.asList("p4-0001", "p1-0002"));
        data.put(new PathAndMax("/r3", 1), Arrays.asList("p2-0001", "p5-0002"));
        data.put(new PathAndMax("/r4", 1), Arrays.asList("p2-0001", "p3-0002"));
        data.put(new PathAndMax("/r5", 1), Arrays.asList("p3-0001", "p4-0002"));

        Exhibitor       mockExhibitor = Mockito.mock(Exhibitor.class);
        PathAnalyzer    pathAnalyzer = new PathAnalyzer(mockExhibitor, Lists.newArrayList(data.keySet()))
        {
            @Override
            protected List<String> getChildren(String path) throws Exception
            {
                List<String> l = data.get(new PathAndMax(path, 0));
                return (l != null) ? l : Lists.<String>newArrayList();
            }

            @Override
            protected String getId(String fullPath) throws Exception
            {
                return fullPath.substring(4, 6);
            }
        };

        Analysis analysis = pathAnalyzer.analyze();

        System.out.println("Details:");
        System.out.println(analysis.getCompleteData());
        System.out.println();
        System.out.println("Cycles:");
        System.out.println(analysis.getPossibleCycles());
    }
}
