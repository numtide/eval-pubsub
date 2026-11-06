set -euEo pipefail

declare _trace _command

while [[ "$#" -gt 0 ]] ; do
    case "$1" in
        --trace)
            _trace=1
            ;;
        *)
            _command="$1"
            ;;
    esac
    shift 1
done

if [[ "${_trace:-}" = 1 ]] ; then
    set -x
fi

declare -A _commands
_commands[run]=_command_run
_commands[psql]=_command_psql

_user="$(id -un)"
_postgres_mount="$FLAKE_ROOT/postgres"
_postgres_initdb_d="$FLAKE_ROOT/postgres/initdb.d"

function _command_psql() {
    psql --dbname pubsub --host "$_postgres_mount" "$@"
}

function _command_run() {
    if ! [[ -d postgres ]] ; then
        mkdir postgres
    fi

    mkdir --parents "$_postgres_initdb_d"

    (
        umask 0002

        cat > "$_postgres_initdb_d/init.sql" <<EOF
CREATE ROLE $_user WITH SUPERUSER LOGIN;
EOF
    )

    echo "> starting postgresql..."
    _container_id="$(
        podman run \
            --replace \
            --detach \
            --log-driver k8s-file \
            --name pubsub-postgres \
            --volume "$_postgres_initdb_d:/docker-entrypoint-initdb.d" \
            --volume "$_postgres_mount:/run/postgresql" \
            --env POSTGRES_PASSWORD=mysecretpassword \
            docker.io/postgres:18.6-alpine
    )"

    while ! _command_psql --command 'select 1;' -tA > /dev/null 2>&1 ; do
        sleep 5
    done
    echo "> postgresql running and available"

    pushd "$FLAKE_ROOT/pubsub-submitter"
    echo "> running diesel"
    diesel database setup
    popd
}

if [[ -n "${_commands[$_command]:-}" ]] ; then
    eval "${_commands[$_command]}"
else
    echo "error"
    exit 1
fi
