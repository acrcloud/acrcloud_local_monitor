FROM ubuntu

RUN apt-get update
RUN apt-get -y install curl
RUN curl --version 
RUN apt-get install python2 --assume-yes
RUN python2 -v
RUN apt-get install -y net-tools iputils-ping screen vim --assume-yes
RUN apt-get install -y build-essential libssl-dev libffi-dev python2-dev --assume-yes
RUN apt-get install -y libpq-dev libxml2-dev libxslt1-dev libldap2-dev libsasl2-dev 
RUN apt-get install -y libmysqlclient-dev --assume-yes
RUN rm -rf /var/lib/apt/lists/*


RUN curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py
RUN python2 ./get-pip.py
RUN pip --version

RUN pip install typing
RUN pip install Twisted==20.3.0 requests fuzzywuzzy beautifulsoup4 python-dateutil psutil
RUN pip install pymysql #mysql-connector mysql-connector-python


WORKDIR /docker_local_monitor
COPY . .

ENTRYPOINT ["/bin/bash", "run.sh"]
