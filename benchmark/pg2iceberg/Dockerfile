# Thin wrapper over the repo-root pg2iceberg image. The Makefile builds
# the base image from the repo root Dockerfile (`--features prod`) and
# passes it as BASE; this layer adds envsubst + the config template so
# the container renders its YAML config from task-definition env vars at
# startup (the binary itself has no env overrides — config is pure YAML).
ARG BASE=pg2iceberg-bench-base:latest
FROM ${BASE}

USER root
# gettext-base: envsubst for the config template.
# curl + jq: entrypoint fetches the Fargate task-role credentials from
# the ECS container-credentials endpoint and exports them as AWS_* env
# vars (iceberg-rust's opendal/reqsign S3 IO only checks env/IMDS, not
# the ECS container endpoint, so it can't authenticate on Fargate alone).
RUN apt-get update \
 && apt-get install -y --no-install-recommends gettext-base curl jq \
 && rm -rf /var/lib/apt/lists/*

COPY config.yaml.tmpl /etc/pg2iceberg/config.yaml.tmpl
COPY entrypoint.sh /usr/local/bin/bench-entrypoint.sh
RUN chmod +x /usr/local/bin/bench-entrypoint.sh

USER pg2iceberg
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/bench-entrypoint.sh"]
CMD ["run", "--config", "/tmp/config.yaml"]
