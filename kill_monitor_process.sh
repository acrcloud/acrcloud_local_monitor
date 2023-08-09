#!/bin/sh
kill -9 $(ps aux | grep '[p]ython3 acrcloud_local_server.py' | awk '{print $2}')

