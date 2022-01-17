FROM golang:1.17-buster AS builder
MAINTAINER Ian Davis <ian.davis@protocol.ai>

ENV SRC_PATH    /build/sentinel-archiver
ENV GO111MODULE on
ENV GOPROXY     https://proxy.golang.org

RUN apt-get update && apt-get install -y ca-certificates

WORKDIR $SRC_PATH
COPY go.* $SRC_PATH/
RUN go mod download

COPY . $SRC_PATH
ARG GOFLAGS
RUN go build $GOFLAGS -trimpath -mod=readonly

#-------------------------------------------------------------------

#------------------------------------------------------
FROM busybox:1-glibc
MAINTAINER Ian Davis <ian.davis@protocol.ai>

ENV SRC_PATH    /build/sentinel-archiver

COPY --from=builder $SRC_PATH/sentinel-archiver /usr/local/bin/sentinel-archiver
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

ENTRYPOINT ["/usr/local/bin/sentinel-archiver"]

CMD [""]
