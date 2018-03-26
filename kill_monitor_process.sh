#!/bin/sh
kill $(ps aux | grep '[p]ython acrcloud_local_server.py' | awk '{print $2}')

