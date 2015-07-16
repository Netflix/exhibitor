Netflix Exhibitor on OpenStack Swift Object Storage
================


## Build

	git clone https://github.com/yanglei99/exhibitor.git
	
	./gradlew clean build install
	
	cd shadow
	
	../gradlew shadowJar
	

## Installation

* upload shadow/build/libs/exhibitor-*-all.jar to the target ZooKeeper nodes

* optional, revise [sample_config.properties](sample_config.properties) and upload to the object storage as default settings

* revise [sample_run.sh](sample_run.sh), and upload to the target ZooKeeper nodes


## Run


Use the revised run script above.


## Verification

Access http://<zk node>:8090/exhibitor/v1/ui/index.html
	
## Configuration Details

Type | Key | Default | Details
--- | --- | --- | --- 
Swift Options | | |
| swiftconfig || The container name and key to store the config. Argument is [container name]:[key].
| swiftconfigprefix |exhibitor-| When using Swift shared config files, the prefix to use for values such as locks.
--- | --- | --- 
Swift Config Options |||
|swiftprovider |openstack-swift | Optional provider for jclouds to use for swiftbackup or swiftconfig.
|swiftidentity | | Optional identify for jcloud to use for swiftbackup or swiftconfig.
|swiftapikey | | Optional api key to use for swiftbackup or swiftconfig.
|swiftauthur l| | Optional authentication url to use for swiftbackup or swiftconfig.
--- | --- | --- 
Swift Backup Options |||
|swiftbackup||If true, enables Swift backup of ZooKeeper
--- | --- | --- 
Swift Backup Configuration |||
|Throttle (bytes/ms)|Integer.toString(1024 * 1024)|Data throttling. Maximum bytes per millisecond
|Swift Container Name||The Swift container to use
|Swfit Key Prefix|exhibitor-backup|The prefix for Swift backup keys
|Max Retries|3|Maximum retries when uploading/downloading Swift data
|Retry Sleep (ms)|1000|Sleep time in milliseconds when retrying



## Known Issue

* Do not support zookeeper repo installation. Download zookeeper jar and unzip. Make sure to revise the location of zookeeper installation either in the property file or through config panel. 