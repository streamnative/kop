FROM golang:1.13.4-alpine

RUN apk -U add ca-certificates git bash build-base sudo  pkgconf build-base
RUN git clone https://github.com/edenhill/librdkafka.git && cd librdkafka && ./configure --prefix /usr && make && make install

WORKDIR /go/src/app
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...

CMD sh -c '/go/bin/golang-confluent-kafka; echo "ExitCode=$?"'