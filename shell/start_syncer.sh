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

usage() {
    echo "
Usage: $0 [--deamon] [--log_level [info|debug]] [--log_dir dir] [--db_dir dir]
          [--host host] [--port port] [--pid_dir dir] [--pprof [true|false]]
          [--pprof_port p_port] [--connect_timeout s] [--rpc_timeout s]
        "
    exit 1
}

OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -o 'h' \
    -l 'help' \
    -l 'daemon' \
    -l 'log_level:' \
    -l 'log_dir:' \
    -l 'db_type:' \
    -l 'db_dir:' \
    -l 'db_host:' \
    -l 'db_port:' \
    -l 'db_user:' \
    -l 'db_password:' \
    -l 'host:' \
    -l 'port:' \
    -l 'pid_dir:' \
    -l 'pprof:' \
    -l 'pprof_port:' \
    -l 'connect_timeout:' \
    -l 'rpc_timeout:' \
    -- "$@")"

eval set -- "${OPTS}"

RUN_DAEMON=0
HOST="127.0.0.1"
PORT="9190"
LOG_LEVEL=""
DB_DIR="${SYNCER_HOME}/db/ccr.db"
DB_TYPE="sqlite3"
DB_HOST="127.0.0.1"
DB_PORT="3306"
DB_USER=""
DB_PASSWORD=""
PPROF="false"
PPROF_PORT="6060"
CONNECT_TIMEOUT="10s"
RPC_TIMEOUT="30s"
while true; do
    case "$1" in
    -h)
        usage
        ;;
    --help)
        usage
        ;;
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
    --db_type)
        DB_TYPE=$2
        shift 2
        ;;
    --db_dir)
        DB_DIR=$2
        shift 2
        ;;
    --db_host)
        DB_HOST=$2
        shift 2
        ;;
    --db_port)
        DB_PORT=$2
        shift 2
        ;;
    --db_user)
        DB_USER=$2
        shift 2
        ;;
    --db_password)
        DB_PASSWORD=$2
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
    --pid_dir)
        PID_DIR=$2
        shift 2
        ;;
    --pprof)
        PPROF=$2
        shift 2
        ;;
    --pprof_port)
        PPROF_PORT=$2
        shift 2
        ;;
    --connect_timeout)
        CONNECT_TIMEOUT=$2
        shift 2
        ;;
    --rpc_timeout)
        RPC_TIMEOUT=$2
        shift 2
        ;;
    --)
        shift
        break
        ;;
    esac
done

export PID_DIR
PID_FILENAME="${HOST}_${PORT}" 

if [[ RUN_DAEMON -eq 0 ]]; then
    if [[ -z "${LOG_LEVEL}" ]]; then
        LOG_LEVEL="trace"
    fi
else
    if [[ -z "${LOG_LEVEL}" ]]; then
        LOG_LEVEL="info"
    fi
fi

if [[ -z "${LOG_DIR}" ]]; then
    LOG_DIR="${SYNCER_HOME}/log/${PID_FILENAME}.log"
fi

pidfile="${PID_DIR}/${PID_FILENAME}.pid"
if [[ -f "${pidfile}" ]]; then
    if kill -0 "$(cat "${pidfile}")" >/dev/null 2>&1; then
        echo "Syncer running as process $(cat "${pidfile}"). Stop it first."
        exit 1
    else
        rm "${pidfile}"
    fi
fi

if [[ -n "${DB_USER}" ]]; then
    if [[ "${DB_TYPE}" == "sqlite3" ]]; then
        echo "sqlite3 is only for local for now"
        exit 1
    fi
fi

chmod 755 "${SYNCER_HOME}/bin/ccr_syncer"
echo "start time: $(date)" >>"${LOG_DIR}"

if [[ "${RUN_DAEMON}" -eq 1 ]]; then
    nohup "${SYNCER_HOME}/bin/ccr_syncer" \
          "-db_dir=${DB_DIR}" \
          "-db_type=${DB_TYPE}" \
          "-db_host=${DB_HOST}" \
          "-db_port=${DB_PORT}" \
          "-db_user=${DB_USER}" \
          "-db_password=${DB_PASSWORD}" \
          "-host=${HOST}" \
          "-port=${PORT}" \
          "-pprof=${PPROF}" \
          "-pprof_port=${PPROF_PORT}" \
          "-log_level=${LOG_LEVEL}" \
          "-log_filename=${LOG_DIR}" \
          "-connect_timeout=${CONNECT_TIMEOUT}" \
          "-rpc_timeout=${RPC_TIMEOUT}" \
          "$@" >>"${LOG_DIR}" 2>&1 </dev/null &
    echo $! > ${pidfile}
else
    "${SYNCER_HOME}/bin/ccr_syncer" \
        "-db_dir=${DB_DIR}" \
        "-db_type=${DB_TYPE}" \
        "-db_host=${DB_HOST}" \
        "-db_port=${DB_PORT}" \
        "-db_user=${DB_USER}" \
        "-db_password=${DB_PASSWORD}" \
        "-host=${HOST}" \
        "-port=${PORT}" \
        "-pprof=${PPROF}" \
        "-pprof_port=${PPROF_PORT}" \
        "-connect_timeout=${CONNECT_TIMEOUT}" \
        "-rpc_timeout=${RPC_TIMEOUT}" \
        "-log_level=${LOG_LEVEL}" | tee -a "${LOG_DIR}"
fi