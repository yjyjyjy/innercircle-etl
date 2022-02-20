#!/bin/bash
#* * * * * cd /home/junyuyang/etl;bash cron_exec.sh
source /home/junyuyang/anaconda3/etc/profile.d/conda.sh
conda activate etl
/home/junyuyang/anaconda3/envs/etl/bin/python daily_update_script.py >> /home/junyuyang/etl/log.txt 2>&1