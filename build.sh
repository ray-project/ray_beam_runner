#!/bin/bash -ex
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

COMPILE_IMG_NAME=raybeamrunner-buildwheel:latest

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

if [ "${IN_DOCKER:-}" = "" ]
then
    # not in docker

    if [ $(docker images | grep $(echo ${COMPILE_IMG_NAME} | sed "s/:.*//g") | wc -l) -eq 0 ]
    then
        echo "building docker image ${COMPILE_IMG_NAME}"
        sudo chown -R $(whoami) ${SCRIPTPATH}
        docker-compose -f ${SCRIPTPATH}/docker/docker-compose.yml build buildwheel-img
    fi

    if [ $(ls ${SCRIPTPATH}/dist/*.whl | wc -l ) -eq 0 ] || [ $(ls ${SCRIPTPATH}/dist/*.egg | wc -l ) -eq 0 ]
    then
        docker container run --rm \
        -e "IN_DOCKER=1" -v "${SCRIPTPATH}:/root/raybeamrunner:rw" \
        ${COMPILE_IMG_NAME} /root/raybeamrunner/build.sh
    fi
    
    if [ $(ls ${SCRIPTPATH}/dist/*.whl | wc -l ) -eq 0 ]
    then
        echo "failed to build wheel file"
        exit 1
    fi

    sudo chown -R $(whoami) ${SCRIPTPATH}/.cache

    docker-compose -f docker/docker-compose.yml build runner-img
else
    # in docker
    pushd /root/raybeamrunner
    python3 ./setup.py bdist_wheel

    [ -e .cache ] || mkdir .cache
    pushd .cache

    if [ ! -e beam ]
    then
        # checkout beam
        git clone https://github.com/apache/beam.git
    fi

    pushd beam
    pip3 install -r sdks/python/build-requirements.txt

    export PATH=$PATH:/usr/local/go/bin
    ./gradlew classes --info

    pushd sdks/python

    rm -rf dist/*.whl
    # compile beam
    python3 ./setup.py bdist_wheel

    # copy wheel file to raybeamrunner dist folder
    cp dist/*.whl /root/raybeamrunner/dist/
    popd # .cache/beam/sdks/python

    popd # .cache/beam
    popd # .cache

    popd
fi

