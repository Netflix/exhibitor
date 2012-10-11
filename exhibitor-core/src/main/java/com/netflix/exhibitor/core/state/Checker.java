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
import com.netflix.exhibitor.core.config.InstanceConfig;
import com.netflix.exhibitor.core.config.StringConfigs;
import java.util.List;

public class Checker
{
    private final Exhibitor exhibitor;
    private final String hostname;

    public Checker(Exhibitor exhibitor)
    {
        this(exhibitor, "localhost");
    }
    
    public Checker(Exhibitor exhibitor, String hostname)
    {
        this.exhibitor = exhibitor;
        this.hostname = hostname;
    }

    public StateAndLeader calculateState() throws Exception
    {
        InstanceConfig          config = exhibitor.getConfigManager().getConfig();

        if ( !isSet(config, StringConfigs.ZOOKEEPER_DATA_DIRECTORY) || !isSet(config, StringConfigs.ZOOKEEPER_INSTALL_DIRECTORY) )
        {
            return new StateAndLeader(InstanceStateTypes.LATENT, false);
        }

        InstanceStateTypes      actualState = InstanceStateTypes.DOWN;
        boolean                 isLeader = false;
        String                  ruok = new FourLetterWord(FourLetterWord.Word.RUOK, hostname, config, exhibitor.getConnectionTimeOutMs()).getResponse();
        if ( "imok".equals(ruok) )
        {
            // The following code depends on inside knowledge of the "srvr" response. If they change it
            // this code might break

            List<String> lines = new FourLetterWord(FourLetterWord.Word.SRVR, hostname, config, exhibitor.getConnectionTimeOutMs()).getResponseLines();
            for ( String line : lines )
            {
                if ( line.contains("not currently serving") )
                {
                    actualState = InstanceStateTypes.NOT_SERVING;
                    break;
                }

                if ( line.toLowerCase().startsWith("mode") )
                {
                    actualState = InstanceStateTypes.SERVING;
                    String[]        parts = line.split(":");
                    if ( parts.length > 1 )
                    {
                        if ( parts[1].trim().equalsIgnoreCase("leader") )
                        {
                            isLeader = true;
                        }
                    }
                    break;
                }
            }
        }

        return new StateAndLeader(actualState, isLeader);
    }

    private boolean isSet(InstanceConfig config, StringConfigs type)
    {
        String s = config.getString(type);
        return (s != null) && (s.trim().length() > 0);
    }
}
