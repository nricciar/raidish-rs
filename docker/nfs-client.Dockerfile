# A Linux NFS client for trying the nodes' NFS servers (privileged: it
# mounts through the host's kernel). See the `nfs-client` service.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends nfs-common netbase \
 && rm -rf /var/lib/apt/lists/*
