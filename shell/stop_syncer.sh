#!/bin/bash

set -eo pipefail

PID_FILES=()
FILENAMES=""
HOST="127.0.0.1"
PORT=""

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

SYNCER_HOME="$(
    cd "${curdir}/.."
    pwd
)"
export SYNCER_HOME

PID_DIR="$(
    cd "${curdir}"
    pwd
)"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
    [no option]         stop all Syncers which pid files in current dir
    --pid_dir           specify the path of pid files
    --files             specify pid files corresponding to Syncers that need to be stopped
    --host & --port     specify host and port corresponding to the Syncer which need to be stopped
  Eg.
    sh stop_syncer.sh --files \"a.pid b.pid c.pid\"
        stop Syncers a, b, c which pid files in current dir
    
    sh stop_syncer.sh --pid_dir /path/to/pid_files --files \"a.pid b.pid c.pid\" 
        stop Syncers a, b, c which pid files in /path/to/pid_files
    
    sh stop_syncer.sh --host 127.0.0.1 --port 9190
        stop Syncers which host info is 127.0.0.1:9190 and pid file is in current dir

    sh stop_syncer.sh --host 127.0.0.1 --port 9190 --pid_dir /path/to/pid_files
        stop Syncers which host info is 127.0.0.1:9190 and pid file is in /path/to/pid_files
    "
    exit 1
}

OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -o 'h' \
    -l 'help' \
    -l 'pid_dir:' \
    -l 'files:' \
    -l 'host:' \
    -l 'port:' \
    -- "$@")"

eval set -- "${OPTS}"

while true; do
    case "$1" in
    --pid_dir)
        PID_DIR=$2
        shift 2
        ;;
    --files)
        FILENAMES=$2
        shift 2
        ;;
    --host)
        HOST=$2
        shift 2
        ;;
    --port)
        PORT=$2
        shift 2
        ;;
    -h)
        usage
        ;;
    --help)
        usage
        ;;
    --)
        shift
        break
        ;;
    esac
done
export PID_DIR

SINGLE_FILENAME=""
if [[ -n "${HOST}" ]] && [[ -n "${PORT}" ]]; then
    SINGLE_FILENAME="${HOST}_${PORT}"
fi

if [[ -n "${FILENAMES}" ]]; then
    IFS=' ' read -ra PID_FILES <<< "${FILENAMES}"
elif [[ -n "${SINGLE_FILENAME}" ]]; then
    PID_FILES+=("${SINGLE_FILENAME}.pid")
else
    for file in "$PID_DIR"/*.pid; do
        PID_FILES+=($(basename "$file"))
    done
fi
echo "${PID_FILES[@]}"

signum=9
if [[ "$1" = "--grace" ]]; then
    signum=15
fi

for name in ${PID_FILES[@]}; do
    pidfile="${PID_DIR}/${name}"

    if [[ -f "${pidfile}" ]]; then
        pid="$(cat "${pidfile}")"

        # check if pid valid
        if test -z "${pid}"; then
            echo "ERROR: invalid pid."
            exit 1
        fi

        # check if pid process exist
        if ! kill -0 "${pid}" 2>&1; then
            echo "ERROR: be process ${pid} does not exist."
            exit 1
        fi

        pidcomm="$(basename "$(ps -p "${pid}" -o comm=)")"
        # check if pid process is backend process
        if [[ "ccr_syncer" != "${pidcomm}" ]]; then
            echo "ERROR: pid process may not be syncer. "
            exit 1
        fi

        # kill pid process and check it
        if kill "-${signum}" "${pid}" >/dev/null 2>&1; then
            while true; do
                if kill -0 "${pid}" >/dev/null 2>&1; then
                    echo "waiting be to stop, pid: ${pid}"
                    sleep 2
                else
                    echo "stop ${pidcomm}, and remove pid file. "
                    if [[ -f "${pidfile}" ]]; then rm "${pidfile}"; fi
                    break
                fi
            done
        else
            echo "ERROR: failed to stop ${pid}"
            exit 1
        fi
    else
        echo "ERROR: ${pidfile} does not exist"
        exit 1
    fi
done