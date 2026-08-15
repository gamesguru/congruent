ARG BASE_IMAGE=ubuntu:latest
FROM ${BASE_IMAGE}
EXPOSE 8008
EXPOSE 8448
RUN apt-get update && apt-get install -y ca-certificates liburing2 libgflags2.2 libnuma1 libtbb12 && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /etc/continuwuity /var/lib/continuwuity /usr/local/bin/
COPY complement/complement-entrypoint.sh /usr/local/bin/complement-entrypoint.sh
COPY complement/complement.config.toml /etc/continuwuity/config.toml
ARG BINARY_PATH=target/latest/conduwuit
COPY ${BINARY_PATH} /usr/local/bin/conduwuit
RUN chmod +x /usr/local/bin/conduwuit /usr/local/bin/complement-entrypoint.sh
ARG UID=1000
ARG GID=1000
ENV CONDUWUIT_UID=${UID}
ENV CONDUWUIT_GID=${GID}
RUN chown -R ${UID}:${GID} /etc/continuwuity /var/lib/continuwuity

#HEALTHCHECK --interval=30s --timeout=5s CMD curl --fail http://localhost:8008/_continuwuity/server_version || exit 1
ENTRYPOINT ["/usr/local/bin/complement-entrypoint.sh"]
