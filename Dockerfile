FROM python:3.7
MAINTAINER Eduard Asriyan <ed-asriyan@protonmail.com>

WORKDIR /application

ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ADD database.py .
ADD serializers.py .
ADD utils.py .
ADD sync_google_drive.py .

CMD python main.py --db-uri $DB_URI