# Broadcast Monitoring - ACRCloud Local Service

## Overview
  [ACRCloud](https://www.acrcloud.com/) provides [Automatic Content Recognition](https://www.acrcloud.com/docs/introduction/automatic-content-recognition/) services for [Audio Fingerprinting](https://www.acrcloud.com/docs/introduction/audio-fingerprinting/) based applications such as **[Audio Recognition](https://www.acrcloud.com/music-recognition)** (supports music, video, ads for both online and offline), **[Broadcast Monitoring](https://www.acrcloud.com/broadcast-monitoring)**, **[Second Screen](https://www.acrcloud.com/second-screen-synchronization)**, **[Copyright Protection](https://www.acrcloud.com/copyright-protection-de-duplication)** and etc.<br>
  
Local **Bradcast Monitoring** System is used to monitor live radio streams on your own local server. [Learn More](https://www.acrcloud.com/docs/acrcloud-services/for-pc-server/radio-airplay-monitoring-music/#server-location)

## Requirements
* Python 2.7
* Works on Linux/Windows
* Follow one of the tutorials to create a project and get your host, access_key and access_secret.

 * [Broadcast Monitoring for Music](https://www.acrcloud.com/docs/tutorials/broadcast-monitoring-for-music/)
 
 * [Broadcast Monitoring for Custom Content](https://www.acrcloud.com/docs/tutorials/broadcast-monitoring-for-custom-content/)


## How To Use
1. You should register an account on the [ACRCloud platform](https://console.acrcloud.com/), and create a [Broadcast Monitoring project](https://www.acrcloud.com/docs/tutorials/broadcast-monitoring-for-music/) with local monitoring type, you will get access_key and access_secret, then add your live radio streams in your project.
2. Clone the code in your local server.
3. Install MySQL, import acrcloud_database.sql to your mysql server. `$mysql -uroot -p < acrcloud_database.sql`.
4. Modify configuration file (acrcloud_config.py), fill access_key, access_secret and database info in the config file.
5. Run `python acrcloud_server.py` to start monitor server.
6. You can use client to refresh, get stream state, pause and restart stream, run `python acrcloud_client.py`.
7. You can use `Ctrl + \` to stop monitor server (in Linux).
8. You can get monitoring results in your MySql database.
9. If you want to record recognize audio, you can set config["record"]["record"]=3 (default is 0 and it means not record).<br>
   You can set config["record"]["record_dir"] to specify the save path.<br>
   You can set config["record"]["record_save_days"] to specify the keep days of record files.<br>
   These config can be set in "acrcloud_config.py".<br>

## Python Dependency Library
1. [Twisted](https://github.com/twisted/twisted)
2. [fuzzywuzzy](https://github.com/seatgeek/fuzzywuzzy)
3. [beautifulsoup4](https://pypi.python.org/pypi/beautifulsoup4)
4. [MySQL-Python](https://pypi.python.org/pypi/MySQL-python)


## Install on Windows

1. Install Windows Runtime Library
    
    * X86: [download and install Library(vcredist_x86.exe)](https://www.microsoft.com/en-us/download/details.aspx?id=5555)
    * x64: [download and install Library(vcredist_x64.exe)](https://www.microsoft.com/en-us/download/details.aspx?id=14632)

2. Install [Mysql](https://dev.mysql.com/downloads/installer/) on your windows
3. Create Databases(in acrcloud_local_monitor direction and run this command)

    `mysql -uroot -p acrcloud_database.sql`

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
