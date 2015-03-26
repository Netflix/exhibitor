package com.netflix.exhibitor.standalone;

public class SSLArguments
{
    private final String keystore ;
    private final String keystorePassword ;
    private final String truststore;
    private final String trustPassword ;

    public SSLArguments()
    {
        this(null, null, null, null);
    }

    public SSLArguments(String keystoreFile, String keystorePwd, String trustStoreFile, String trustStorePwd)
    {
        this.keystore = keystoreFile;
        this.keystorePassword = keystorePwd;
        this.truststore = trustStoreFile;
        this.trustPassword = trustStorePwd;
    }

    public String getKeystore() {
		return keystore;
	}

	public String getKeystorePassword() {
		return keystorePassword;
	}

	public String getTruststore() {
		return truststore;
	}

	public String getTrustPassword() {
		return trustPassword;
	}
}
