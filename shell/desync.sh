#!/bin/bash

# ./cli -h $host -p $port --j $job_name
usage() {
    echo "Usage: $0 [-h host] [-p port] [-j job]"
    echo ""
    echo "Options:"
    echo "  -h host    the ccr syncer host, default is 127.0.0.1"
    echo "  -p port    the ccr syncer port, default is 9190"
    echo "  -j job     the job name"
    exit 1
}

# parse cli args
host="127.0.0.1"
port="9030"
job=""
while getopts ":h:p:j:" opt; do
    case $opt in
        h) host="$OPTARG"
        ;;
        p) port="$OPTARG"
        ;;
        j) job="$OPTARG"
        ;;
        \?) echo "Invalid option -$OPTARG" >&2
        usage
        ;;
    esac
done

if [ -z "$job" ]; then
    echo "the job name is empty"
    exit 1
fi

# check if jq command exists
if ! [ -x "$(command -v jq)" ]; then
    echo "Error: jq is not installed." >&2
    echo "You can install jq by running: sudo apt-get install jq" >&2
    exit 1
fi

echo "${host}:${port} Pause job ${job} ..."
while [ true ]; do
    # check until the curl result is json ok
    response=$(curl -s -X POST -H "Content-Type: application/json" \
        -d "{\"name\":\"${job}\"}" http://${host}:${port}/pause)
    if [ $? -ne 0 ]; then
        echo "${host}:${port} Failed to pause job ${job}, retry after 3s" >&2
        sleep 3
        continue
    fi

    state=$(echo $response | jq -r '.success')
    if [ "$state" == "true" ]; then
        echo "${host}:${port} Pause job ${job} success"
        break
    fi

    error_msg=$(echo $response | jq -r '.error_msg')
    echo "${host}:${port} Pause job ${job} failed, error: ${error_msg}" >&2
    exit 1
done

echo "${host}:${port} Desync the job ${job} ..."
while [ true ]; do
    # Check until the curl result is json ok
    response=$(curl -s -X POST -H "Content-Type: application/json" \
        -d "{\"name\":\"${job}\"}" http://${host}:${port}/desync)
    if [ $? -ne 0 ]; then
        echo "${host}:${port} Failed to desync job ${job}, retry after 3s" >&2
        sleep 3
        continue
    fi
    
    state=$(echo $response | jq -r '.success')
    if [ "$state" == "true" ]; then
        echo "${host}:${port} Desync job ${job} success"
        break
    fi

    error_msg=$(echo $response | jq -r '.error_msg')
    echo "${host}:${port} Desync job ${job} failed, error: ${error_msg}" >&2
    exit 1
done
