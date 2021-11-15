FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive 

RUN apt-get -y update && apt-get -y upgrade && apt-get -y install curl npm
RUN curl --silent --location https://deb.nodesource.com/setup_16.x | bash -
RUN apt-get install --yes build-essential nodejs && npm install --global yarn

COPY . /src
RUN cd /src; yarn

EXPOSE 3000

WORKDIR /src
CMD ["/usr/bin/yarn", "start"]
