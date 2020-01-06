FROM node:8-alpine

RUN apk add yarn snappy

RUN apk --no-cache add \
      bash \
      g++ \
      ca-certificates \
      lz4-dev \
      musl-dev \
      cyrus-sasl-dev \
      openssl-dev \
      make \
      python

RUN apk add --no-cache --virtual .build-deps gcc zlib-dev libc-dev bsd-compat-headers py-setuptools bash


# Create app directory
WORKDIR /usr/src/app

COPY . .

RUN yarn install

CMD sh -c 'node main.js; echo "ExitCode=$?"'