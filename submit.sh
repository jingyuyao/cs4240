#!/usr/bin/env bash

gcloud dataproc jobs submit spark --class cs4240.Main --jars target/scala-2.11/cs4240.jar "$@"
