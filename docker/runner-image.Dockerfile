FROM python:3.9.13-bullseye

RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-setuptools vim less

COPY dist/*.whl root/
RUN pip install root/*.whl

COPY requirements_dev.txt root/requirements.txt
RUN pip install -r root/requirements.txt
