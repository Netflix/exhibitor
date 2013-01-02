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
