#!/bin/bash

java -Djclouds.keystone.credential-type=tempAuthCredentials  -jar exhibitor-1.5.6-SNAPSHOT-all.jar -c swift --hostname $(hostname) --port 8090 --swiftbackup true --swiftidentity <your account> --swiftapikey <your api key> --swiftauthurl https://<your datacenter>.objectstorage.softlayer.net/auth/v1.0 --swiftconfig myexhibitor:myconfig.properties