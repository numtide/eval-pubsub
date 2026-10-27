if ! [[ -d postgres ]] ; then
    mkdir postgres
fi
podman run \
       --replace \
       --detach \
       --volume "$FLAKE_ROOT/postgres:/run/postgresql" \
       --name pubsub-postgres \
       --env POSTGRES_PASSWORD=mysecretpassword \
       docker.io/postgres:18.6-alpine
