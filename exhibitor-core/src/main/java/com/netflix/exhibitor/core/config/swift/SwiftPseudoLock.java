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

package com.netflix.exhibitor.core.config.swift;

import com.netflix.exhibitor.core.config.PseudoLockBase;

import java.util.ArrayList;
import java.util.List;

import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteArrayPayload;
import org.jclouds.openstack.swift.v1.domain.ObjectList;
import org.jclouds.openstack.swift.v1.features.ObjectApi;

public class SwiftPseudoLock extends PseudoLockBase
{
	private final ObjectApi objectApi;

	/**
	 * 
	 * @param objectApi ObjectApi
	 * @param lockPrefix key prefix
	 * @param timeoutMs max age for locks
	 * @param pollingMs how often to poll S3
	 */
    public SwiftPseudoLock(ObjectApi objectApi, String lockPrefix, int timeoutMs, int pollingMs)
    {
        super(lockPrefix, timeoutMs, pollingMs);
        this.objectApi = objectApi;
    }

    /**
     * 
     * @param objectApi ObjectApi
     * @param lockPrefix key prefix
     * @param timeoutMs max age for locks
     * @param pollingMs how often to poll S3
     * @param settlingMs how long to wait to reach consistency
     */
    public SwiftPseudoLock(ObjectApi objectApi, String lockPrefix, int timeoutMs, int pollingMs, int settlingMs)
    {
        super(lockPrefix, timeoutMs, pollingMs, settlingMs);
        this.objectApi = objectApi;
    }


    @Override
    protected void createFile(String key, byte[] contents) throws Exception
    {
	    Payload payload =  new ByteArrayPayload(contents);
		objectApi.put(key, payload);
    }
    
    @Override
    protected void deleteFile(String key) throws Exception
    {
    	objectApi.delete(key);
    }

    @Override
    protected List<String> getFileNames(String lockPrefix) throws Exception
    {
    	// TODO find a better way to get files by prefix
	    ObjectList list = objectApi.list();
	    List<String> names = new ArrayList<String>();
	    for (int i=0; i< list.size(); i++)
	    {
	    	String name = list.get(i).getName();
	    	if (name.startsWith(lockPrefix))
	    		names.add(name);
	    }
	    return names;
    }
}
