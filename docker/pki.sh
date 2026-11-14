#!/bin/sh
# Certificates for the compose cluster, in /pki (./docker/pki on the host):
# a CA, one bundle per node and an admin bundle for the CLI. Runs once; to
# start over, delete ./docker/pki/ca.key (or the whole directory).
set -e
if [ -e /pki/ca.key ]; then
    echo "pki: CA exists"
else
    raidish cert init --dir /pki --name "raidish compose CA"
    # The client config lists every node by URL: one cert per host.
    raidish cert issue-nodes --dir /pki
    raidish cert issue admin --role admin --dir /pki
fi
# Give the files to whoever owns the directory on the host, so the admin
# key is usable outside the containers too.
chown -R "$(stat -c %u:%g /pki)" /pki
raidish cert status --dir /pki
