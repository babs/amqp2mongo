FROM node:20

WORKDIR /app

COPY amqp2mongo.js package*.json Dockerfile ./

RUN set -e \
  && npm ci

USER node

CMD ["amqp2mongo.js"]
