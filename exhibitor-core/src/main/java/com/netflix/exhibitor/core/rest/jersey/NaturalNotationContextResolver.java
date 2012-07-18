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

package com.netflix.exhibitor.core.rest.jersey;

import com.netflix.exhibitor.core.entities.*;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

@Provider
class NaturalNotationContextResolver implements ContextResolver<JAXBContext>
{
    private JAXBContext context;

    NaturalNotationContextResolver()
    {
        try
        {
            this.context = new JSONJAXBContext
            (
                JSONConfiguration.natural().build(),
                Index.class,
                Result.class,
                SearchId.class,
                SearchRequest.class,
                SearchResult.class,
                UITabSpec.class,
                NameAndModifiedDate.class,
                PathAnalysis.class,
                PathAnalysisNode.class,
                PathAnalysisRequest.class,
                IdList.class,
                UsageListingRequest.class,
                FieldValue.class
            );
        }
        catch ( JAXBException e )
        {
            throw new RuntimeException(e);
        }
    }

    public JAXBContext getContext(Class<?> objectType)
    {
        return context;
    }
}