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
export PID_DIR

signum=9
if [[ "$1" = "--grace" ]]; then
    signum=15
fi

pidfile="${PID_DIR}/syncer.pid"

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
                exit 0
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