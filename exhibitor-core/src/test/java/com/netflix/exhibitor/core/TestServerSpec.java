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
