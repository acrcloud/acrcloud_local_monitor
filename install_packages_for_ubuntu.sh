#!/bin/sh

apt-get update
apt-get install -y net-tools iputils-ping screen vim --assume-yes
apt-get install -y build-essential libssl-dev libffi-dev python3-dev --assume-yes
apt-get install -y libpq-dev libxml2-dev libxslt1-dev libldap2-dev libsasl2-dev
apt-get install -y libmysqlclient-dev --assume-yes

curl -sS https://bootstrap.pypa.io/get-pip.py | python3

pip3 install typing
pip3 install psutil
pip3 install pymysql
pip3 install Twisted
pip3 install requests
pip3 install fuzzywuzzy
pip3 install beautifulsoup4
pip3 install python-dateutil
