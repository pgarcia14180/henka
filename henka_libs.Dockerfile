FROM python:3.8
WORKDIR /usr/src/app
RUN chmod -R 777 /usr/src/app

COPY requirements.txt .
RUN pip install -r ./requirements.txt 
RUN rm requirements.txt
RUN pip install boto3
RUN pip install PyYAML
