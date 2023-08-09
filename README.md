# Broadcast Monitoring - ACRCloud Local Service

## Overview
a  [ACRCloud](https://www.acrcloud.com/) provides services such as **[Music Recognition](https://www.acrcloud.com/music-recognition)**, **[Broadcast Monitoring](https://www.acrcloud.com/broadcast-monitoring/)**, **[Custom Audio Recognition](https://www.acrcloud.com/second-screen-synchronization%e2%80%8b/)**, **[Copyright Compliance & Data Deduplication](https://www.acrcloud.com/copyright-compliance-data-deduplication/)**, **[Live Channel Detection](https://www.acrcloud.com/live-channel-detection/)**, and **[Offline Recognition](https://www.acrcloud.com/offline-recognition/)** etc.<br>
  
## Requirements                                                                                                                             
Faollow one of the tutorials to create a project and get your host, access_key and access_secret.
 * Python 3
 * Works on Linux
 * Follow one of the tutorials to create a project and get your host, access_key.
 
 * [Recognize Music](https://docs.acrcloud.com/tutorials/recognize-music)
 * [Recognize Custom Content](https://docs.acrcloud.com/tutorials/recognize-custom-content)
 * [Broadcast Monitoring for Music](https://docs.acrcloud.com/tutorials/broadcast-monitoring-for-music)
 * [Broadcast Monitoring for Custom Content](https://docs.acrcloud.com/tutorials/broadcast-monitoring-for-custom-content)
 * [Detect Live & Timeshift TV Channels](https://docs.acrcloud.com/tutorials/detect-live-and-timeshift-tv-channels)
 * [Recognize Custom Content Offline](https://docs.acrcloud.com/tutorials/recognize-custom-content-offline)
 * [Recognize Live Channels and Custom Content](https://docs.acrcloud.com/tutorials/recognize-tv-channels-and-custom-content)
  
Local **Bradcast Monitoring** System is used to monitor live radio streams on your own local server. [Learn More](https://www.acrcloud.com/docs/acrcloud-services/for-pc-server/radio-airplay-monitoring-music/#server-location)

## How To Use
1. You should register an account on the [ACRCloud platform](https://console.acrcloud.com/), and create a [Broadcast Monitoring project](https://www.acrcloud.com/docs/tutorials/broadcast-monitoring-for-music/) with local monitoring type, you will get access_key, then add your live radio streams in your project.
2. Clone the code in your local server.
3. If using Mysql, install MySQL, import acrcloud_database.sql to your mysql server. `$mysql -uroot -p < acrcloud_database.sql`.
4. Modify configuration file (acrcloud_config.py), fill access_key, access_secret and database info in the config file.
5. Run `python3 acrcloud_local_server.py` to start monitor server.
6. You can use client to refresh, get stream state, pause and restart stream, run `python3 acrcloud_client.py`.
7. You can use `Ctrl + \` to stop monitor server (in Linux).
8. You can get monitoring results in your MySql database.
9. If you want to record recognize audio, you can set config["record"]["record"]=3 (default is 0 and it means not record).<br>
   You can set config["record"]["record_dir"] to specify the save path.<br>
   You can set config["record"]["record_save_days"] to specify the keep days of record files.<br>
   These config can be set in "acrcloud_config.py".<br>

## Python Dependency Library
1. [Twisted](https://github.com/twisted/twisted)
2. [requests](https://pypi.org/project/requests/)
3. [fuzzywuzzy](https://github.com/seatgeek/fuzzywuzzy)
4. [beautifulsoup4](https://pypi.python.org/pypi/beautifulsoup4)
5. [pymysql](https://pypi.org/project/pymysql/)



## Run as a Docker Container

1. Install Docker

    * If you are using MacOs: Download [Docker Desktop for Mac](https://download.docker.com/mac/stable/Docker.dmg) and install.
    * If you are using Linux: Open the Terminal and input `bash <(curl -s https://get.docker.com/)`

2. Run following command

    * `git clone https://github.com/acrcloud/acrcloud_local_monitor`

    * `cd acrcloud_local_monitor`

    *  Change the config file "acrcloud_config.py", fill in your project "access_key", and mysql setting(host, port, user, passwd), **if Mysql was installed in your computer, please create a remote user and set field "host" to your private IP(ifconfig in Linux or ipconfig in Windows, "host.docker.internal" in Mac OS)**

    * `docker build -t acrcloud/acr_local_monitor_image:v1 .`

    * `docker run --rm -itv /<Change to your directory>/acrcloud_local_monitor:/docker_local_monitor --name="acr_local_monitor" acrcloud/acr_local_monitor_image:v1`
