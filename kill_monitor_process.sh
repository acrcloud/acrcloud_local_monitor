#!/bin/sh
kill $(ps aux | grep '[p]ython acrcloud_server.py' | awk '{print $2}')

