#!/bin/bash
#* * * * * cd /home/junyuyang/etl;bash cron_exec.sh
/usr/bin/python3 address_metadata_worker.py >> /home/junyuyang/etl/address_metadata/logs/log.txt 2>&1