package com.netflix.exhibitor.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.jersey.api.client.filter.ClientFilter;
import java.util.Collection;

public class RemoteConnectionConfiguration
{
    private final Collection<ClientFilter> filters;
    private final int connectionTimeoutMs;
    private final int readTimeoutMs;

    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 10000;
    private static final int DEFAULT_READ_TIMEOUT_MS = 3000;

    public RemoteConnectionConfiguration()
    {
        this(Lists.<ClientFilter>newArrayList(), DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    public RemoteConnectionConfiguration(Collection<ClientFilter> filters)
    {
        this(filters, DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
    }

    public RemoteConnectionConfiguration(Collection<ClientFilter> filters, int connectionTimeoutMs, int readTimeoutMs)
    {
        this.filters = ImmutableList.copyOf(Preconditions.checkNotNull(filters, "filters cannot be null"));
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    public Collection<ClientFilter> getFilters()
    {
        return filters;
    }

    public int getConnectionTimeoutMs()
    {
        return connectionTimeoutMs;
    }

    public int getReadTimeoutMs()
    {
        return readTimeoutMs;
    }
}
