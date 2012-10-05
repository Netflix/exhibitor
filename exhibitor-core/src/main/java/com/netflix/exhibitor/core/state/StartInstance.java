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

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.Activity;
import com.netflix.exhibitor.core.activity.ActivityLog;

public class StartInstance implements Activity
{
    private final Exhibitor exhibitor;

    public StartInstance(Exhibitor exhibitor)
    {
        this.exhibitor = exhibitor;
    }

    @Override
    public void completed(boolean wasSuccessful)
    {
        // NOP
    }

    @Override
    public Boolean call() throws Exception
    {
        exhibitor.getLog().add(ActivityLog.Type.INFO, "Attempting to start instance");

        boolean     success = false;
        try
        {
            exhibitor.getProcessOperations().startInstance();
            success = true;
        }
        catch ( Exception e )
        {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Trying to kill start instance", e);
        }
        return success;
    }
}
