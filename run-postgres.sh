if ! [[ -d postgres ]] ; then
    mkdir postgres
fi
podman run \
       --replace \
       --volume "$FLAKE_ROOT/postgres:/run/postgresql" \
       --name pubsub-postgres \
       --env POSTGRES_PASSWORD=mysecretpassword \
       postgres:18.6-alpine
