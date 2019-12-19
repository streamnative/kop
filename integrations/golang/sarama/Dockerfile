FROM golang:1.13.4-stretch

WORKDIR /go/src/app
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...

CMD [ "/go/bin/sarama-golang" ]