FROM python:3.7
MAINTAINER Eduard Asriyan <ed-asriyan@protonmail.com>

WORKDIR /application

ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ADD database.py .
ADD serializers.py .
ADD synchronizers.py .
ADD utils.py .

ADD sync_yadisk.py .

CMD while true; do python sync_yadisk.py --db-uri $DB_URI --token $TOKEN; sleep 86400; done