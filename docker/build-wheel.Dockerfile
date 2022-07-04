FROM python:3.9.13-bullseye

RUN apt update && \
    apt install -y python3-setuptools python3-pip git \
    default-jdk vim less \
    gradle maven wget python3-virtualenv \
    rsync less 
    #openjdk-11-jdk

WORKDIR /root

RUN wget https://go.dev/dl/go1.18.3.linux-amd64.tar.gz
RUN rm -rf /usr/local/go && tar -C /usr/local -xzf go1.18.3.linux-amd64.tar.gz
