FROM mcr.microsoft.com/devcontainers/base:bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
