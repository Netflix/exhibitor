package com.netflix.exhibitor.core.config.couchdb;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;
import org.lightcouch.Document;
import org.lightcouch.NoDocumentException;
import org.lightcouch.Response;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.activity.ActivityLog.Type;
import com.netflix.exhibitor.core.config.ConfigCollection;
import com.netflix.exhibitor.core.config.ConfigProvider;
import com.netflix.exhibitor.core.config.LoadedInstanceConfig;
import com.netflix.exhibitor.core.config.PropertyBasedInstanceConfig;
import com.netflix.exhibitor.core.config.PseudoLock;

public class CouchdbConfigProvider implements ConfigProvider 
{

	public static final String DOC_ID = "exhibitorCollection";
	private CouchDbClient dbClient;
	public static Properties defaults;
	private String hostname;
	private String user;
	private String pass;
	
	public CouchdbConfigProvider(CouchdbConfigAruguments args, Properties defaults) 
	{
		this.hostname = args.getHostname();
		this.user = args.getUsername();
		this.pass = args.getPassword();
		CouchdbConfigProvider.defaults = defaults;
	}
	
	@Override
	public void close() throws IOException 
	{
		dbClient.shutdown();
	}

	@Override
	public void start() throws Exception 
	{
		System.err.println("starting cloudant");
		CouchDbProperties properties = new CouchDbProperties()
		  .setDbName("exhibitor")
		  .setCreateDbIfNotExist(true)
		  .setProtocol("https")
		  .setHost(hostname)
		  .setPort(443)
		  .setUsername(user)
		  .setPassword(pass)
		  .setMaxConnections(100)
		  .setConnectionTimeout(0);
		dbClient = new CouchDbClient(properties);
	}

	@Override
	public LoadedInstanceConfig loadConfig() throws Exception 
	{
		try {
			ExhibitorDocument d =  dbClient.find(ExhibitorDocument.class, DOC_ID);
			return new LoadedInstanceConfig(d.getConfig(), d.getRevision().hashCode());
		} catch ( NoDocumentException e ) {
			// noop
		}

        PropertyBasedInstanceConfig config = new PropertyBasedInstanceConfig(new Properties(), defaults);
        return new LoadedInstanceConfig(config, 0);
	}

	@Override
	public LoadedInstanceConfig storeConfig(ConfigCollection config, long compareVersion) throws Exception 
	{
		PropertyBasedInstanceConfig propertyBasedInstanceConfig = new PropertyBasedInstanceConfig(config);
		Response response;
		
		response = dbClient.contains(DOC_ID) ?	
					updateWith(propertyBasedInstanceConfig, compareVersion) : 
					saveWith(propertyBasedInstanceConfig);
		
		return new LoadedInstanceConfig(propertyBasedInstanceConfig, response.getRev().hashCode());
	}
	
	private Response updateWith(PropertyBasedInstanceConfig config, long compareVersion) throws Exception
	{
		ExhibitorDocument d = dbClient.find(ExhibitorDocument.class, DOC_ID);
		int version = d.getRevision().hashCode();
		if( version != compareVersion )
		{
			return null;
		}
		d.setConfig(config);
		
		return dbClient.update(d);
	}
	
	private Response saveWith(PropertyBasedInstanceConfig config) throws Exception
	{
		ExhibitorDocument d = new ExhibitorDocument();
		d.setConfig(config);
		
		return dbClient.save(d);
	}

	@Override
	public PseudoLock newPseudoLock() throws Exception 
	{
		return new CouchdbLock();
	}
	
	class CouchdbLock implements PseudoLock
	{
		private final String KEY = "lock:" + UUID.randomUUID().toString();
		private long timeout;
		
		@Override
		public boolean lock(ActivityLog log, long maxWait, TimeUnit unit)
				throws Exception 
		{
			long        startMs = System.currentTimeMillis();
		    boolean     hasMaxWait = (unit != null);
		    long        maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;
		    timeout = startMs + maxWaitMs;
		    
			while ( dbClient.contains(KEY) )
			{
				Thread.sleep(250);
				if ( maxWaitExceeded() ) 
				{ 
					log.add(Type.ERROR, "failed to acquire lock");
					return false; 
				}
			}
			
			JsonObject json = new JsonObject();
			json.addProperty("_id", KEY);
			dbClient.save(json);
			return true;
		}
		
		private boolean maxWaitExceeded()
		{
			return System.currentTimeMillis() < timeout;
		}

		@Override
		public void unlock() throws Exception 
		{
			if ( dbClient.contains(KEY))
			{
				JsonObject doc = dbClient.find(JsonObject.class, KEY);
				dbClient.remove(doc);
			}
			
		}
	}
}

class ExhibitorDocument extends Document 
{
	@SerializedName("properties")
	Properties p;

	public ExhibitorDocument() 
	{
		setId(CouchdbConfigProvider.DOC_ID);
	}

	public PropertyBasedInstanceConfig getConfig() throws Exception 
	{
		PropertyBasedInstanceConfig config = new PropertyBasedInstanceConfig(p, CouchdbConfigProvider.defaults);
		return config;
	}

	public void setConfig(PropertyBasedInstanceConfig config) throws Exception 
	{
		 p = config.getProperties();
	}
}

