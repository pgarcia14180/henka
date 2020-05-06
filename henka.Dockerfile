FROM henka_libs
WORKDIR /usr/src/app

RUN mkdir /usr/local/lib/python3.8/site-packages/henka
COPY ./henka /usr/local/lib/python3.8/site-packages/henka
