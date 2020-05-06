FROM python:3.7
RUN apt-get update -y 
RUN apt-get install zip
WORKDIR /usr/src

RUN chmod -R 777 /usr/src

RUN mkdir henka
COPY ./henka ./henka/
COPY requirements.txt .
COPY ./s3_file_uploader* .
RUN mkdir -p /henka/python/lib/python3.7/site-packages
RUN cp -a henka /henka/python/lib/python3.7/site-packages/
RUN pip install -r ./requirements.txt --target /libs_folder/python/lib/python3.7/site-packages
WORKDIR /libs_folder
RUN zip -r henka_libs_layer.zip .
RUN cp /libs_folder/henka_libs_layer.zip /usr/src/henka_libs_layer.zip
WORKDIR /henka
RUN zip -r henka.zip .
RUN cp /libs_folder/henka_libs_layer.zip /usr/src/henka_libs_layer.zip
RUN cp /henka/henka.zip /usr/src/henka.zip
WORKDIR /usr/src
RUN pip install boto3
RUN pip install PyYAML
RUN python s3_uploader.py
