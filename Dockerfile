FROM node:20-slim

WORKDIR /app

COPY amqp2mongo.js package*.json Dockerfile ./

RUN set -e \
  && npm ci --omit=dev

USER node

CMD ["node", "amqp2mongo.js"]
