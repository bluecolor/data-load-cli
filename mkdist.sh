#!/bin/bash
sbt assembly
rm -f dist/lauda.jar dist/log.properties
cp target/scala-2.13/lauda-assembly-0.1.0-SNAPSHOT.jar dist/lauda.jar
cp log.properties dist/
cp config/lauda.yaml dist/
touch dist/lauda.sh
echo "java -Dlog4j.configuration=file:\"./log.properties\"  -jar lauda.jar \$*" > dist/lauda.sh
chmod +x dist/lauda.sh