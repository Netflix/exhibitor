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
	

## Known Issue

* Do not support zookeeper repo installation. Download zookeeper jar and unzip. Make sure to revise the location of zookeeper installation either in the property file or through config panel. 