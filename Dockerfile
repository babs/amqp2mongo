FROM node:16

WORKDIR /app

COPY amqp2mongo.js package*.json Dockerfile yarn.lock ./

RUN set -e \
  && yarn install

CMD ["node", "amqp2mongo.js"]
