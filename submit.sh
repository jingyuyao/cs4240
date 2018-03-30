#!/usr/bin/env bash

sbt package
gcloud dataproc jobs submit spark --class cs4240.Main --jars target/scala-2.11/cs4240.jar --cluster $1
