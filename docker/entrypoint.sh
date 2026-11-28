#!/bin/sh
# Create this node's disk (a sparse file) on first start, get its
# certificate from the cluster CA if it has none yet, then run raidish.
#
# The certificate (see docker-compose.yml), with RAIDISH_CONFIG and
# RAIDISH_PORT naming the node's config and port:
#   RAIDISH_CA_INIT=1          make the cluster CA (`raidish ca init`):
#                              node0's certificate, an admin one and the
#                              peers' join tokens, all in /pki
#   RAIDISH_JOIN=<url>         join the cluster at <url> with the token in
#                              /pki/tokens/node-<id>.token (RAIDISH_NODE_ID)
set -e
disk=${RAIDISH_DISK:-/data/disk}
if [ ! -e "$disk" ]; then
    mkdir -p "$(dirname "$disk")"
    truncate -s "${DISK_SIZE:-1G}" "$disk"
    echo "created $disk (${DISK_SIZE:-1G})"
fi
cfg=${RAIDISH_CONFIG:-/etc/raidish/node.yaml}
tls=${RAIDISH_TLS:-/etc/raidish/tls}
if [ -n "$RAIDISH_CA_INIT" ] && [ ! -e "$tls/cert.pem" ]; then
    echo "ca: making the cluster CA"
    mkdir -p /pki/tokens
    # The recovery keys are in what it prints: the owner's only.
    (umask 077; raidish -c "$cfg" ca init --san "$(hostname)" --port "${RAIDISH_PORT:-8881}" \
        --admin-out /pki/admin --root-out /pki/root.enc --tokens-out /pki/tokens \
        > /pki/ca-init.txt)
    cp "$tls/ca.pem" /pki/ca.pem
    # The host user gets the files (the admin key is for the CLI there).
    chown -R "$(stat -c %u:%g /pki)" /pki
    echo "ca: done; recovery keys and join commands in docker/pki/ca-init.txt"
fi
if [ -n "$RAIDISH_JOIN" ] && [ ! -e "$tls/cert.pem" ]; then
    token=/pki/tokens/node-${RAIDISH_NODE_ID}.token
    echo "ca: waiting for $token"
    i=0
    until [ -s "$token" ]; do
        i=$((i + 1)); [ "$i" -ge 120 ] && { echo "ca: no token" >&2; exit 1; }
        sleep 1
    done
    i=0
    until raidish -c "$cfg" join "$RAIDISH_JOIN" --token-file "$token" --port "${RAIDISH_PORT:-8881}"; do
        i=$((i + 1)); [ "$i" -ge 30 ] && exit 1
        sleep 2
    done
    chown -R "$(stat -c %u:%g /pki/tokens)" "$tls"
fi
exec raidish "$@"
