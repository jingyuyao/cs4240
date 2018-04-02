#!/usr/bin/env bash

gcloud compute ssh --zone=$1 $2 -- -D 1080 -N -n