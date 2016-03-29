# ACRCloud Local Monitoring Service

# Overview
Local Monitoring Services is used to monitor live radio streams in your local server.

# Requirements
* Python 2.7
* Works on Linux

# How To Use
1. You should register an account on the [ACRCloud platform](https://console.acrcloud.com/), and create an project with local type in Broadcast Monitoring, you will get access_key and access_secret, then add your live radio streams in your project.
2. Clone the code in your local server.
3. Install MySQL, Run command `source acrcloud_database.sql;` in mysql.
4. Modify configuration file (acrcloud_config.py), fill in your database configuration.
5. Run `python acrcloud_server.py`

## Python Dependency Library
1. [Twisted](https://github.com/twisted/twisted)
2. [fuzzywuzzy](https://github.com/seatgeek/fuzzywuzzy)
3. [beautifulsoup4](https://pypi.python.org/pypi/beautifulsoup4)
4. [MySQL-Python](https://pypi.python.org/pypi/MySQL-python)

