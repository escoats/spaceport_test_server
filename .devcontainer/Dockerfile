FROM mcr.microsoft.com/devcontainers/base:bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 make protobuf-compiler python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Keep Python packages separate from Debian's managed Python installation.
# PATH makes this environment available without manual activation.
ENV VIRTUAL_ENV=/opt/spaceport-venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"
RUN python3 -m venv "${VIRTUAL_ENV}" \
    && chown -R vscode:vscode "${VIRTUAL_ENV}"
