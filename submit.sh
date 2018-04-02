#!/usr/bin/env bash

sbt assembly
gcloud dataproc jobs submit spark --class cs4240.Main --jars target/scala-2.11/cs4240.jar --region $1 --cluster $2
