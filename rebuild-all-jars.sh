#!/bin/sh
# Standardizes how we build all the jars.  Requires that you install maven (I've been using 3.5.3 on jdk 1.8.0_162).
# This should get called from the root directory of bee-proof to build

echo '>>>> BUILDING EMR-3:'
mvn clean package -P emr-3 && cp target/bee-proof-1.0.0-emr-3.jar lib/
echo
echo

echo '>>>> BUILDING EMR-4:'
mvn clean package -P emr-4 && cp target/bee-proof-1.0.0-emr-4.jar lib/
echo
echo

echo '>>>> BUILDING EMR-5:'
mvn clean package -P emr-5 && cp target/bee-proof-1.0.0-emr-5.jar lib/
echo
echo

echo 'FINISHED'
echo
echo "Make sure you check above for success of each profile's build!!!"
