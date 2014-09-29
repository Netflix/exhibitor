package com.netflix.exhibitor.core.config.couchdb;

public class CouchdbConfigAruguments {
	private String hostname;
	private String username;
	private String password;

	public CouchdbConfigAruguments(String hostname, String username, String password) 
	{
		this.hostname = hostname;
		this.username = username;
		this.password = password;
	}
	
	public String getHostname() 
	{
		return hostname;
	}

	public void setHostname(String hostname) 
	{
		this.hostname = hostname;
	}

	public String getUsername() 
	{
		return username;
	}

	public void setUsername(String username) 
	{
		this.username = username;
	}

	public String getPassword() 
	{
		return password;
	}

	public void setPassword(String password) 
	{
		this.password = password;
	}
}
