curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'daemon' \
    -- "$@")"

eval set -- "${OPTS}"

RUN_DAEMON=0
while true; do
    case "$1" in
    --daemon)
        RUN_DAEMON=1
        shift
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error, opt: $1"
        exit 1
        ;;
    esac
done

SYNCER_HOME="$(
    cd "${curdir}/.."
    pwd
)"
export SYNCER_HOME

export LOG_DIR="${SYNCER_HOME}/log"
PID_DIR="$(
    cd "${curdir}"
    pwd
)"
export PID_DIR


pidfile="${PID_DIR}/be.pid"
if [[ -f "${pidfile}" ]]; then
    if kill -0 "$(cat "${pidfile}")" >/dev/null 2>&1; then
        echo "Syncer running as process $(cat "${pidfile}"). Stop it first."
        exit 1
    else
        rm "${pidfile}"
    fi
fi

chmod 755 "${SYNCER_HOME}/lib/ccr_syncer"
echo "start time: $(date)" >>"${LOG_DIR}/ccr_syncer.out"

if [[ "${RUN_DAEMON}" -eq 1 ]]; then
    nohup "${SYNCER_HOME}/lib/ccr_syncer" "$@" >>"${LOG_DIR}/ccr_syncer.out" 2>&1 </dev/null &
    echo $! > bin/syncer.pid
else
    "${SYNCER_HOME}/lib/ccr_syncer" "$@" 2>&1 </dev/null
fi