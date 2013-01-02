/*
 * Copyright 2013 Netflix, Inc.
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

package com.netflix.exhibitor.core.controlpanel;

import org.testng.Assert;
import org.testng.annotations.Test;
import java.io.File;
import java.util.prefs.Preferences;

public class TestFileBasedPreferences
{
    @Test
    public void testBasic() throws Exception
    {
        File               tempFile = File.createTempFile("temp", ".temp");
        try
        {
            Preferences        preferences = new FileBasedPreferences(tempFile);

            Assert.assertNull(preferences.get("test", null));
            Assert.assertFalse(preferences.getBoolean("b", false));

            preferences.put("test", "value");
            preferences.putBoolean("b", true);

            Assert.assertEquals(preferences.get("test", null), "value");
            Assert.assertTrue(preferences.getBoolean("b", false));
            preferences.flush();
            Assert.assertEquals(preferences.get("test", null), "value");
            Assert.assertTrue(preferences.getBoolean("b", false));

            preferences = new FileBasedPreferences(tempFile);
            Assert.assertEquals(preferences.get("test", null), "value");
            Assert.assertTrue(preferences.getBoolean("b", false));
        }
        finally
        {
            //noinspection ResultOfMethodCallIgnored
            tempFile.delete();
        }
    }
}
