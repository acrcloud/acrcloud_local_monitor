FROM ubuntu

RUN apt-get update
RUN apt-get install -y net-tools iputils-ping screen vim --assume-yes
RUN apt-get install -y python-pip build-essential libssl-dev libffi-dev python-dev --assume-yes
RUN apt-get install -y libmysqlclient-dev --assume-yes
RUN rm -rf /var/lib/apt/lists/*

RUN pip install twisted requests fuzzywuzzy beautifulsoup4 python-dateutil psutil
RUN pip install MySQL-Python

WORKDIR /docker_local_monitor

