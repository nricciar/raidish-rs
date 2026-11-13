#!/bin/sh
# One-shot setup for the compose cluster: format the pool if it isn't yet,
# then create a demo volume. Safe to run again.

retry() { # tries, command...
    n=$1; shift
    i=0
    until "$@"; do
        i=$((i + 1))
        [ "$i" -ge "$n" ] && return 1
        sleep 2
    done
}

if raidish load >/dev/null 2>&1; then
    echo "init: pool already formatted"
else
    # The committers step aside (release the pool lease) while the pool is
    # unformatted, so format gets it within a few tries.
    echo "init: formatting the pool"
    retry 30 raidish format || { echo "init: format failed" >&2; exit 1; }
fi

# Volumes go through the committer, once one has loaded the pool.
if raidish volume list 2>/dev/null | grep -qw demo0; then
    echo "init: demo0 exists"
else
    echo "init: creating volume demo0 (64 MiB)"
    retry 30 raidish volume create demo0 67108864 || { echo "init: volume create failed" >&2; exit 1; }
fi
raidish volume list
raidish cluster status
echo "init: done. Dashboard: http://node0:8881/ (or :8882 / :8883)"
