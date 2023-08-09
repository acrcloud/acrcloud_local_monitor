#!/bin/sh

cd "/home/acrcloud_moitor/acrcloud_local_monitor"
screen -d -m -S "acrcloud_monitor"
screen -S "acrcloud_monitor" -p 0 -X stuff $'python3 acrcloud_local_server.py\n'
