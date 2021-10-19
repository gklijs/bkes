FROM rust:1.51 as builder

ARG PROTOC_ZIP=protoc-3.15.6-linux-x86_64.zip
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v3.15.6/${PROTOC_ZIP} &&\
    unzip -o ${PROTOC_ZIP} -d /usr/local bin/protoc &&\
    unzip -o ${PROTOC_ZIP} -d /usr/local 'include/*' &&\
    rm ${PROTOC_ZIP}
RUN rustup component add rustfmt
RUN apt-get update &&\
    apt-get install -y build-essential &&\
    apt-get install -y cmake
WORKDIR /workdir
COPY ./ ./
RUN cargo build --release &&\
    mkdir -p /build-out &&\
    cp target/release/bkes /build-out/

FROM ubuntu:21.10

COPY --from=builder /build-out/bkes /
CMD /bkes
EXPOSE 50030