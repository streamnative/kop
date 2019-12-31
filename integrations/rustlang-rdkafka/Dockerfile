FROM rust:1.40-stretch

RUN apt update
RUN apt install -y cmake libc6-dev

WORKDIR /usr/src/rustlang-rdkafka
COPY . .

RUN cargo install --path .

CMD sh -c 'rustlang-rdkafka; echo "ExitCode=$?"'