# Broadcast Monitoring - ACRCloud Local Service

## Overview
  [ACRCloud](https://www.acrcloud.com/) provides [Automatic Content Recognition](https://www.acrcloud.com/docs/introduction/automatic-content-recognition/) services for [Audio Fingerprinting](https://www.acrcloud.com/docs/introduction/audio-fingerprinting/) based applications such as **[Audio Recognition](https://www.acrcloud.com/music-recognition)** (supports music, video, ads for both online and offline), **[Broadcast Monitoring](https://www.acrcloud.com/broadcast-monitoring)**, **[Second Screen](https://www.acrcloud.com/second-screen-synchronization)**, **[Copyright Protection](https://www.acrcloud.com/copyright-protection-de-duplication)** and etc.<br>
  
Local **Bradcast Monitoring** System is used to monitor live radio streams on your own local server. [Learn More](https://www.acrcloud.com/docs/acrcloud-services/for-pc-server/radio-airplay-monitoring-music/#server-location)

## Requirements
* Python 2.7
* Works on Linux
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

