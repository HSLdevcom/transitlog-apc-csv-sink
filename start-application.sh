#!/bin/sh

if [[ "${DEBUG_ENABLED}" = true ]]; then
  java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 -XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=95.0 -jar /usr/app/transitlog-apc-archive-sink.jar
else
  java -XX:InitialRAMPercentage=50.0 -XX:MaxRAMPercentage=95.0 -jar /usr/app/transitlog-apc-archive-sink.jar
fi
