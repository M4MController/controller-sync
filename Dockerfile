FROM python:3.7
MAINTAINER Eduard Asriyan <ed-asriyan@protonmail.com>

WORKDIR /application

ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ADD core/database.py .
ADD core/serializers.py .
ADD core/stores.py .
ADD core/utils.py .

ADD sync_yadisk.py .

CMD while true; do python sync_yadisk.py --db-uri $DB_URI --token $TOKEN --key $KEY; sleep 86400; done