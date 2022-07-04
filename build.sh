#!/bin/bash -ex

COMPILE_IMG_NAME=raybeamrunner-buildwheel:latest

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

if [ "${IN_DOCKER:-}" = "" ]
then
    # not in docker
    rm -rf ${SCRIPTPATH}/dist

    if [ $(docker images | grep $(echo ${COMPILE_IMG_NAME} | sed "s/:.*//g") | wc -l) -eq 0 ]
    then
        echo "building docker image ${COMPILE_IMG_NAME}"
        docker-compose -f ${SCRIPTPATH}/docker/docker-compose.yml build buildwheel-img
    fi

    docker container run --rm \
        -e "IN_DOCKER=1" -v "${SCRIPTPATH}:/root:rw" \
        ${COMPILE_IMG_NAME} /root/build.sh
    
    if [ ! -e ${SCRIPTPATH}/dist/*.whl ]
    then
        echo "failed to build wheel file"
        exit 1
    fi

    docker-compose -f docker/docker-compose.yml build runner-img
else
    # in docker
    pushd /root
    python3 ./setup.py bdist_wheel
    popd
fi

