FROM python:3.7
MAINTAINER Eduard Asriyan <ed-asriyan@protonmail.com>

WORKDIR /application

ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ADD core ./core
ADD sync_yadisk.py .

CMD while true; do sleep 60; python sync_yadisk.py --db-uri $DB_URI; sleep 86400; done