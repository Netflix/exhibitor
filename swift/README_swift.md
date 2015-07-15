Netflix Exhibitor on OpenStack Swift Object Storage
================


## Build

	git clone https://github.com/yanglei99/exhibitor
	
	./gradlew clean build install
	cd shadow
	../gradlew shadowJar
	
* copy exhibitor-1.5.6-SNAPSHOT-all.jar from build/libs to your target zookeeper nodes


## Installation

* upload shadow/build/libs/exhibitor-*-all.jar to the target ZooKeeper nodes

* optional, revise[sample_config.properties](sample_config.properties) and upload to object storage

* revise [sample_run.sh](sample_run.sh), and upload to target ZooKeeper nodes


## Run


Use the run script above.


## Verification

Access http://<zk node>:8090/exhibitor/v1/ui/index.html
	

## Known Issue

* Do not support zookeeper repo installation. Download yourself then revise the location of zookeeper installation either in the property file or through config panel. 