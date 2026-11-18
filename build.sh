ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
export SYNCER_HOME="${ROOT}"

OPTS="$(getopt \
    -n "$0" \
    -o 'j:' \
    -l 'clean' \
    -l 'output:' \
    -- "$@")"

eval set -- "${OPTS}"

SYNCER_OUTPUT="${SYNCER_HOME}/output"
PARALLEL="$(($(nproc) / 4 + 1))"
while true; do
    case "$1" in
    -j)
        PARALLEL="$2"
        shift 2
        ;;
    --clean)
        CLEAN=1
        shift
        ;;
    --output)
        SYNCER_OUTPUT="$2"
        shift 2
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error, opt: $OPTS"
        exit 1
        ;;
    esac
done

export GOMAXPROCS=${PARALLEL}

mkdir -p ${SYNCER_OUTPUT}/bin
mkdir -p ${SYNCER_OUTPUT}/log
mkdir -p ${SYNCER_OUTPUT}/db

if [[ "${CLEAN}" -eq 1 ]]; then
    rm -rf ${SYNCER_HOME}/bin
    exit 0
fi

make ccr_syncer

cp ${SYNCER_HOME}/bin/ccr_syncer ${SYNCER_OUTPUT}/bin/
cp ${SYNCER_HOME}/shell/* ${SYNCER_OUTPUT}/bin/
cp -r ${SYNCER_HOME}/doc ${SYNCER_OUTPUT}/
cp ${SYNCER_HOME}/CHANGELOG.md ${SYNCER_OUTPUT}/
cp ${SYNCER_HOME}/README.md ${SYNCER_OUTPUT}/
