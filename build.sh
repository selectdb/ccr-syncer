ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
export SYNCER_HOME="${ROOT}"

SYNCER_OUTPUT="${SYNCER_HOME}/output"

PARALLEL="$(($(nproc) / 4 + 1))"
while true; do
    case "$1" in
    --clean)
        CLEAN=1
        shift
        ;;
    -j)
        PARALLEL="$2"
        shift 2
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
        shift
        break
        ;;
    esac
done

if [[ ! -d ${SYNCER_OUTPUT} ]]; then
    mkdir ${SYNCER_OUTPUT}
fi

if [[ "${CLEAN}" -eq 1 ]]; then
    rm -rf ${SYNCER_HOME}/lib
    exit 0
fi

make -j ${PARALLEL}

cp -r ${SYNCER_HOME}/lib ${SYNCER_OUTPUT}/lib
cp -r ${SYNCER_HOME}/bin ${SYNCER_OUTPUT}/bin
mkdir ${SYNCER_OUTPUT}/log

