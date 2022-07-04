FROM ubuntu:latest

ARG WHEEL_FILE=ray_beam-0.0.1-py3-none-any.whl

RUN apt-get update && apt-get install -y \
    python3 python3-pip
COPY dist/${WHEEL_FILE} root/${WHEEL_FILE}
RUN pip install root/${WHEEL_FILE}
COPY requirements_dev.txt root/requirements.txt
RUN pip install -r root/requirements.txt
