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

mkdir -p ${SYNCER_OUTPUT}/log

if [[ "${CLEAN}" -eq 1 ]]; then
    rm -rf ${SYNCER_HOME}/bin
    exit 0
fi

make -j ${PARALLEL}

cp -r ${SYNCER_HOME}/bin ${SYNCER_OUTPUT}/bin
cp ${SYNCER_HOME}/shell/* ${SYNCER_OUTPUT}/bin/



