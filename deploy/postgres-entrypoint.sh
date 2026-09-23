#!/bin/sh
set -eu
# Generated once in a private Docker volume; never stored in the repository.
# Only postgres, API and scheduler mount this volume. Workers cannot read it.
if [ ! -s /run/db-auth/password ]; then
    umask 022
    od -An -N32 -tx1 /dev/urandom | tr -d ' \n' > /run/db-auth/password.tmp
    mv /run/db-auth/password.tmp /run/db-auth/password
fi
export POSTGRES_PASSWORD_FILE=/run/db-auth/password
exec /usr/local/bin/docker-entrypoint.sh "$@"
