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

package com.netflix.exhibitor.core.state;

import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Arrays;

import static com.netflix.exhibitor.core.state.ServerType.STANDARD;

public class TestServerList
{
    @Test
    public void     testSimple()
    {
        ServerList      serverList = new ServerList("1:a");
        Assert.assertEquals(serverList.getSpecs(), Arrays.asList(new ServerSpec("a", 1, STANDARD)));

        serverList = new ServerList("1:a,2:b");
        Assert.assertEquals(serverList.getSpecs(), Arrays.asList(new ServerSpec("a", 1, STANDARD), new ServerSpec("b", 2, STANDARD)));

        serverList = new ServerList("2:b,1:a");
        Assert.assertEquals(serverList.getSpecs(), Arrays.asList(new ServerSpec("b", 2, STANDARD), new ServerSpec("a", 1, STANDARD)));

        serverList = new ServerList("2:b,1:a");
        Assert.assertNotEquals(serverList.getSpecs(), Arrays.asList(new ServerSpec("a", 1, STANDARD), new ServerSpec("b", 2, STANDARD)));
    }

    @Test
    public void     testEdges()
    {
        ServerList      serverList = new ServerList("1:a,,,a,asfklasf,L:1x30,:*&*,$:$");
        Assert.assertEquals(serverList.getSpecs(), Arrays.asList(new ServerSpec("a", 1, STANDARD)));

        serverList = new ServerList("1 :a    , 2 :    b \t\t,     , : ,");
        Assert.assertEquals(serverList.getSpecs(), Arrays.asList(new ServerSpec("a", 1, STANDARD), new ServerSpec("b", 2, STANDARD)));
    }
}
