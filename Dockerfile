FROM ubuntu:22.04

RUN apt-get update
RUN apt-get -y install curl
RUN curl --version 
RUN apt-get install python3.10 --assume-yes
RUN ln -s /usr/bin/python3.10 /usr/bin/python3
RUN python3 -v
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3
RUN apt-get install -y net-tools iputils-ping screen vim --assume-yes
RUN apt-get install -y build-essential libssl-dev libffi-dev python3-dev --assume-yes
RUN apt-get install -y libpq-dev libxml2-dev libxslt1-dev libldap2-dev libsasl2-dev 
RUN apt-get install -y libmysqlclient-dev --assume-yes
RUN rm -rf /var/lib/apt/lists/*


RUN pip3 install typing Twisted requests fuzzywuzzy beautifulsoup4 python-dateutil psutil pymysql

WORKDIR /docker_local_monitor
COPY . .

ENTRYPOINT ["/bin/bash", "run.sh"]
