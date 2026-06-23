FROM node:20-slim

WORKDIR /app

COPY amqp2mongo.js package*.json Dockerfile ./

RUN set -e \
  && npm ci --omit=dev

# OCI metadata — declared late so CI build-arg churn (timestamp/commit) doesn't
# bust the npm ci layer cache. CI overrides these; defaults are for local builds.
ARG BUILD_TIMESTAMP="1970-01-01T00:00:00+00:00"
ARG COMMIT_HASH="00000000-dirty"
ARG PROJECT_URL="https://github.com/babs/amqp2mongo"
ARG VERSION="v0.0.0"
ARG BUILDER="local"

LABEL org.opencontainers.image.source=${PROJECT_URL}
LABEL org.opencontainers.image.created=${BUILD_TIMESTAMP}
LABEL org.opencontainers.image.version=${VERSION}
LABEL org.opencontainers.image.revision=${COMMIT_HASH}

USER node

CMD ["node", "amqp2mongo.js"]
