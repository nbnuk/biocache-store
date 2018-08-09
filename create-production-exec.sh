#!/bin/sh

mvn clean install -DskipTests=true -DinitialMemorySize=2g -DmaxMemorySize=4g
cp target/biocache-store-*-distribution.zip ./
