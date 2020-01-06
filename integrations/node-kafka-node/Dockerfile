FROM node:13-alpine

RUN apk add yarn snappy

# Create app directory
WORKDIR /usr/src/app

COPY . .

RUN yarn install

CMD sh -c 'node main.js; echo "ExitCode=$?"'