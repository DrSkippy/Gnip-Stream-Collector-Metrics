#!/usr/bin/env bash

pub=twitter

export PYTHON_PATH=PYTHON_PATH:~/Gnip-Stream-Collector-Metrics/src

nohup ~/Gnip-Stream-Collector-Metrics/src/GnipStreamCollectorMetrics.py --name=$pub &
