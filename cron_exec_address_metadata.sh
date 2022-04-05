#!/bin/bash
#* * * * * cd /home/junyuyang/etl;bash cron_exec.sh
cd /home/junyuyang/etl/address_metadata;/usr/bin/python3 address_metadata_worker.py >> /home/junyuyang/etl/address_metadata/logs/log.txt 2>&1