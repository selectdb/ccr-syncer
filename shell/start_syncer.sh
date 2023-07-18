set -eo pipefail

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

OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'daemon' \
    -l 'log_level:' \
    -l 'log_dir:' \
    -l 'db_dir:' \
    -- "$@")"

eval set -- "${OPTS}"

RUN_DAEMON=0
LOG_LEVEL=""
LOG_DIR="${SYNCER_HOME}/log/ccr_syncer.log"
DB_DIR="${SYNCER_HOME}/db/ccr.db"
while true; do
    case "$1" in
    --daemon)
        RUN_DAEMON=1
        shift
        ;;
    --log_level)
        LOG_LEVEL=$2
        shift 2
        ;;
    --log_dir)
        LOG_DIR=$2
        shift 2
        ;;
    --db_dir)
        DB_DIR=$2
        shift 2
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

if [[ RUN_DAEMON -eq 0 ]]; then
    if [[ -z "${LOG_LEVEL}" ]]; then
        LOG_LEVEL="trace"
    fi
else
    if [[ -z "${LOG_LEVEL}" ]]; then
        LOG_LEVEL="info"
    fi
fi

pidfile="${PID_DIR}/syncer.pid"
if [[ -f "${pidfile}" ]]; then
    if kill -0 "$(cat "${pidfile}")" >/dev/null 2>&1; then
        echo "Syncer running as process $(cat "${pidfile}"). Stop it first."
        exit 1
    else
        rm "${pidfile}"
    fi
fi

chmod 755 "${SYNCER_HOME}/bin/ccr_syncer"
echo "start time: $(date)" >>"${LOG_DIR}"

if [[ "${RUN_DAEMON}" -eq 1 ]]; then
    nohup "${SYNCER_HOME}/bin/ccr_syncer" \
          "-db_dir=${DB_DIR}" \
          "-log_level=${LOG_LEVEL}" \
          "-log_filename=${LOG_DIR}" \
          "$@" >>"${LOG_DIR}" 2>&1 </dev/null &
    echo $! > ${PID_DIR}/syncer.pid
else
    "${SYNCER_HOME}/bin/ccr_syncer" "-db_dir=${DB_DIR}" "-log_level=${LOG_LEVEL}" | tee -a "${LOG_DIR}"
fi