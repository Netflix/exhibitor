package com.netflix.exhibitor.standalone;

public class SecurityArguments
{
    private final String securityFile;
    private final String realmSpec;
    private final String remoteAuthSpec;

    public SecurityArguments()
    {
        this(null, null, null);
    }

    public SecurityArguments(String securityFile, String realmSpec, String remoteAuthSpec)
    {
        this.securityFile = securityFile;
        this.realmSpec = realmSpec;
        this.remoteAuthSpec = remoteAuthSpec;
    }

    public String getSecurityFile()
    {
        return securityFile;
    }

    public String getRealmSpec()
    {
        return realmSpec;
    }

    public String getRemoteAuthSpec()
    {
        return remoteAuthSpec;
    }
}
