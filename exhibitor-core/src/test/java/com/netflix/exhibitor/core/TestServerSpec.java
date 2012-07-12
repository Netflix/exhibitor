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

package com.netflix.exhibitor.core;

import com.netflix.exhibitor.core.state.ServerSpec;
import com.netflix.exhibitor.core.state.ServerType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestServerSpec
{
    @Test
    public void testSpecString()
    {
        ServerSpec      s1 = new ServerSpec("host", 1, ServerType.STANDARD);
        ServerSpec      s2 = new ServerSpec("host", 2, ServerType.OBSERVER);

        Assert.assertEquals(s1.toSpecString(), "1:host");
        Assert.assertEquals(s2.toSpecString(), "O:2:host");
    }
}
