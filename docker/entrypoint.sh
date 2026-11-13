#!/bin/sh
# Create this node's disk (a sparse file) on first start, then run raidish.
set -e
disk=${RAIDISH_DISK:-/data/disk}
if [ ! -e "$disk" ]; then
    mkdir -p "$(dirname "$disk")"
    truncate -s "${DISK_SIZE:-1G}" "$disk"
    echo "created $disk (${DISK_SIZE:-1G})"
fi
exec raidish "$@"
