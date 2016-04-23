# ACRCloud Local Monitoring Service

# Overview
Local Monitoring Services is used to monitor live radio streams in your local server.

# Requirements
* Python 2.7
* Works on Linux

# How To Use
1. You should register an account on the [ACRCloud platform](https://console.acrcloud.com/), and create a [Broadcast Monitoring project](https://docs.acrcloud.com/broadcast-monitoring) with local monitoring type, you will get access_key and access_secret, then add your live radio streams in your project.
2. Clone the code in your local server.
3. Install MySQL, import acrcloud_database.sql to your mysql server. `$mysql -uroot -p < acrcloud_database.sql`.
4. Modify configuration file (acrcloud_config.py), fill access_key, access_secret and database info in the config file.
5. Run `python acrcloud_server.py` to start monitor server.
6. You can use client to refresh, get stream state, pause and restart stream, run `python acrcloud_client.py`.
7. You can use `Ctrl + \` to stop monitor server (in Linux).
8. You can get monitoring results in your MySql database.

## Python Dependency Library
1. [Twisted](https://github.com/twisted/twisted)
2. [fuzzywuzzy](https://github.com/seatgeek/fuzzywuzzy)
3. [beautifulsoup4](https://pypi.python.org/pypi/beautifulsoup4)
4. [MySQL-Python](https://pypi.python.org/pypi/MySQL-python)

