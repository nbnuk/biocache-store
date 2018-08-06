#!/bin/sh

mvn clean install -DskipTests=true -DinitialMemorySize=4g -DmaxMemorySize=4g
cp target/biocache-store-*-distribution.zip ./
