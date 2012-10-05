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

package com.netflix.exhibitor.core.config;

import com.google.common.io.Files;
import com.netflix.exhibitor.core.config.none.NoneConfigProvider;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class TestZooCfgOrdering
{
    @Test
    public void testZooCfgOrdering() throws Exception
    {
        final String          CFG_EXTRA =
            "initLimit=30\n" +
            "syncLimit=30\n" +
            "tickTime=2000\n" +
            "aaa=test";
        File                  tempDirectory = Files.createTempDir();
        ConfigProvider        config = new NoneConfigProvider(tempDirectory.getPath());

        Properties properties = new Properties();
        properties.setProperty(PropertyBasedInstanceConfig.toName(StringConfigs.ZOO_CFG_EXTRA, PropertyBasedInstanceConfig.ROOT_PROPERTY_PREFIX), CFG_EXTRA);
        LoadedInstanceConfig        instanceConfig = config.storeConfig(new PropertyBasedInstanceConfig(properties, new Properties()), 0);

        String               string = instanceConfig.getConfig().getRootConfig().getString(StringConfigs.ZOO_CFG_EXTRA);
        Assert.assertEquals(CFG_EXTRA, string);

        EncodedConfigParser     parser = new EncodedConfigParser("s=10&d=dee&a=hey");
        Assert.assertEquals(parser.getFieldValues().size(), 3);
        Assert.assertEquals(parser.getFieldValues().get(0), new EncodedConfigParser.FieldValue("s", "10"));
        Assert.assertEquals(parser.getFieldValues().get(1), new EncodedConfigParser.FieldValue("d", "dee"));
        Assert.assertEquals(parser.getFieldValues().get(2), new EncodedConfigParser.FieldValue("a", "hey"));

        Map<String, String>     sortedMap = parser.getSortedMap();
        Iterator<Map.Entry<String,String>> iterator = sortedMap.entrySet().iterator();
        Assert.assertEquals(sortedMap.size(), 3);
        Assert.assertEquals(iterator.next(), new HashMap.SimpleEntry<String, String>("a", "hey"));
        Assert.assertEquals(iterator.next(), new HashMap.SimpleEntry<String, String>("d", "dee"));
        Assert.assertEquals(iterator.next(), new HashMap.SimpleEntry<String, String>("s", "10"));
    }
}
