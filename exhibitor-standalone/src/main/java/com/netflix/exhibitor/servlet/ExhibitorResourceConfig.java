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

package com.netflix.exhibitor.servlet;

import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.rest.UIContext;
import com.netflix.exhibitor.core.rest.jersey.JerseySupport;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.servlet.ServletContext;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExhibitorResourceConfig extends ResourceConfig
{
    @SuppressWarnings("FieldCanBeLocal")
    private final Logger                    log = LoggerFactory.getLogger(getClass());
    private final DefaultResourceConfig     config;

    public ExhibitorResourceConfig(@Context ServletContext context)
    {
        DefaultResourceConfig   localConfig;
        Exhibitor               exhibitor = (Exhibitor)context.getAttribute(ExhibitorServletContextListener.class.getName());
        if ( exhibitor != null )
        {
            log.info("Adding Exhibitor Jersey resources");
            localConfig = JerseySupport.newApplicationConfig(new UIContext(exhibitor));
        }
        else
        {
            log.info("Using DefaultResourceConfig");
            localConfig = new DefaultResourceConfig();
        }
        config = localConfig;
    }

    @Override
    public Set<Class<?>> getClasses()
    {
        return config.getClasses();
    }

    @Override
    public Set<Object> getSingletons()
    {
        return config.getSingletons();
    }

    @Override
    public Map<String, MediaType> getMediaTypeMappings()
    {
        return config.getMediaTypeMappings();
    }

    @Override
    public Map<String, String> getLanguageMappings()
    {
        return config.getLanguageMappings();
    }

    @Override
    public Map<String, Object> getExplicitRootResources()
    {
        return config.getExplicitRootResources();
    }

    @Override
    public Map<String, Boolean> getFeatures()
    {
        return config.getFeatures();
    }

    @Override
    public boolean getFeature(String featureName)
    {
        return config.getFeature(featureName);
    }

    @Override
    public Map<String, Object> getProperties()
    {
        return config.getProperties();
    }

    @Override
    public Object getProperty(String propertyName)
    {
        return config.getProperty(propertyName);
    }

    @Override
    public void validate()
    {
        config.validate();
    }

    @Override
    public Set<Class<?>> getRootResourceClasses()
    {
        return config.getRootResourceClasses();
    }

    @Override
    public Set<Class<?>> getProviderClasses()
    {
        return config.getProviderClasses();
    }

    @Override
    public Set<Object> getRootResourceSingletons()
    {
        return config.getRootResourceSingletons();
    }

    @Override
    public Set<Object> getProviderSingletons()
    {
        return config.getProviderSingletons();
    }

    @Override
    public List getContainerRequestFilters()
    {
        return config.getContainerRequestFilters();
    }

    @Override
    public List getContainerResponseFilters()
    {
        return config.getContainerResponseFilters();
    }

    @Override
    public List getResourceFilterFactories()
    {
        return config.getResourceFilterFactories();
    }

    @Override
    public void setPropertiesAndFeatures(Map<String, Object> entries)
    {
        config.setPropertiesAndFeatures(entries);
    }

    @Override
    public void add(Application app)
    {
        config.add(app);
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public ResourceConfig clone()
    {
        return config.clone();
    }
}
