FROM quay.io/pypa/manylinux_2_24_x86_64:latest

RUN apt update && \
    apt install -y python3-setuptools python3-pip
