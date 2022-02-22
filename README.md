# Broadcast Monitoring - ACRCloud Local Service

## Overview
a  [ACRCloud](https://www.acrcloud.com/) provides services such as **[Music Recognition](https://www.acrcloud.com/music-recognition)**, **[Broadcast Monitoring](https://www.acrcloud.com/broadcast-monitoring/)**, **[Custom Audio Recognition](https://www.acrcloud.com/second-screen-synchronization%e2%80%8b/)**, **[Copyright Compliance & Data Deduplication](https://www.acrcloud.com/copyright-compliance-data-deduplication/)**, **[Live Channel Detection](https://www.acrcloud.com/live-channel-detection/)**, and **[Offline Recognition](https://www.acrcloud.com/offline-recognition/)** etc.<br>
  
## Requirements                                                                                                                             
Faollow one of the tutorials to create a project and get your host, access_key and access_secret.
 * Py    thon 2.7
 * Works on Linux/Windows
 * Follow one of the tutorials to create a project and get your host, access_key.
 
 * [Recognize Music](https://docs.acrcloud.com/tutorials/recognize-music)
 * [Recognize Custom Content](https://docs.acrcloud.com/tutorials/recognize-custom-content)
 * [Broadcast Monitoring for Music](https://docs.acrcloud.com/tutorials/broadcast-monitoring-for-music)
 * [Broadcast Monitoring for Custom Content](https://docs.acrcloud.com/tutorials/broadcast-monitoring-for-custom-content)
 * [Detect Live & Timeshift TV Channels](https://docs.acrcloud.com/tutorials/detect-live-and-timeshift-tv-channels)
 * [Recognize Custom Content Offline](https://docs.acrcloud.com/tutorials/recognize-custom-content-offline)
 * [Recognize Live Channels and Custom Content](https://docs.acrcloud.com/tutorials/recognize-tv-channels-and-custom-content)
  
Local **Bradcast Monitoring** System is used to monitor live radio streams on your own local server. [Learn More](https://www.acrcloud.com/docs/acrcloud-services/for-pc-server/radio-airplay-monitoring-music/#server-location)

I
## How To Use
1. You s    hould register an account on the [ACRCloud platform](https://console.acrcloud.com/), and create a [Broadcast Monitoring project](https://www.acrcloud.com/docs/tutorials/broadcast-monitoring-for-music/) with local monitoring type, you will get access_key, then add your live radio streams in your project.
2. Clone the code in your local server.
3. Install MySQL, import acrcloud_database.sql to your mysql server. `$mysql -uroot -p < acrcloud_database.sql`.
4. Modify configuration file (acrcloud_config.py), fill access_key, access_secret and database info in the config file.
5. Run `python acrcloud_local_server.py` to start monitor server.
6. You can use client to refresh, get stream state, pause and restart stream, run `python acrcloud_client.py`.
7. You can use `Ctrl + \` to stop monitor server (in Linux).
8. You can get monitoring results in your MySql database.
9. If you want to record recognize audio, you can set config["record"]["record"]=3 (default is 0 and it means not record).<br>
   You can set config["record"]["record_dir"] to specify the save path.<br>
   You can set config["record"]["record_save_days"] to specify the keep days of record files.<br>
   These config can be set in "acrcloud_config.py".<br>

## Python Dependency Library
1.a [Twisted](https://github.com/twisted/twisted)
2. [requests](https://pypi.org/project/requests/)
3. [fuzzywuzzy](https://github.com/seatgeek/fuzzywuzzy)
4. [beautifulsoup4](https://pypi.python.org/pypi/beautifulsoup4)
5. [MySQL-Python](https://pypi.python.org/pypi/MySQL-python)


## Install on Windows
a
1. Install Windows Runtime Library
    
    * X86: [download and install Library(vcredist_x86.exe)](https://github.com/acrcloud/acrcloud_sdk_python/blob/master/windows/vcredist_x86.exe)
    * x64: [download and install Library(vcredist_x64.exe)](https://github.com/acrcloud/acrcloud_sdk_python/blob/master/windows/vcredist_x64.exe)

2. Install [Mysql](https://dev.mysql.com/downloads/installer/) on your windows
3. Create Databases(in acrcloud_local_monitor direction and run this command)

    `mysql -uroot -p < acrcloud_database.sql`

4. Install [Python](https://www.python.org/downloads/)
5. Install Python Dependency Library(in acrcloud_local_monitor direction and run the script)
    
    `.\install_python_packages_for_windows.bat`

6. Install [MySQL-python](https://pypi.org/project/MySQL-python/1.2.5/)
    
    * Open [https://www.lfd.uci.edu/~gohlke/pythonlibs/](https://www.lfd.uci.edu/~gohlke/pythonlibs/#mysql-python)
    * Find and Download:

        `MySQL_python‑1.2.5‑cp27‑none‑win32.whl (or MySQL_python‑1.2.5‑cp27‑none‑win_amd64.whl)`

    * `python -m pip install MySQL_python‑1.2.5‑cp27‑none‑win32.whl`
    * `python -m pip install mysql-python`        

7. Copy the ACRCloud Library into the acrcloud_local_monitor direction according to your system.

    * X86: copy from winlibs/win32/acrcloud_stream_decode.pyd to acrcloud_local_monitor direction
    * X64: copy from winlibs/win64/acrcloud_stream_decode.pyd to acrcloud_local_monitor direction

8. If you have created Local Project and add some streams, config the acrcloud_config.py with your access_key, and mysql information, then start the local server

    `python acrcloud_local_server.py`

9. Stop the local monitor server

    `python acrcloud_stop.py` and Ctrl-C


## Run as a Docker Container
a
1. Install Docker

    * If you are using Windows: Download [Docker Desktop for Windows](https://download.docker.com/win/stable/Docker%20for%20Windows%20Installer.exe) and install.
    * If you are using MacOs: Download [Docker Desktop for Mac](https://download.docker.com/mac/stable/Docker.dmg) and install.
    * If you are using Linux: Open the Terminal and input bash <(curl -s https://get.docker.com/)

2. Run following command

    * `git clone https://github.com/acrcloud/acrcloud_local_monitor`

    * `cd acrcloud_local_monitor`

    *  Change the config file "acrcloud_config.py", fill in your project "access_key", and mysql setting(host, port, user, passwd), **if Mysql was installed in your computer, please create a remote user and set field "host" to your private IP(ifconfig in Linux or ipconfig in Windows, "host.docker.internal" in Mac OS)**

    * `docker build -t acrcloud/acr_local_monitor_image:v1 .`

    * `docker run --rm -itv /<Change to your directory>/acrcloud_local_monitor:/docker_local_monitor --name="acr_local_monitor" acrcloud/acr_local_monitor_image:v1`
