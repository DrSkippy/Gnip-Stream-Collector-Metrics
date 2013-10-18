#!/usr/bin/env bash

pub=twitter

##
## make sure your packages actually live in these locations
##
export PYTHONPATH=${PYTHONPATH}:~/Gnip-Stream-Collector-Metrics/src:~/Gnacs/acscsv

nohup ~/Gnip-Stream-Collector-Metrics/src/GnipStreamCollectorMetrics.py --name=$pub &
