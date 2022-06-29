ARG ARCH=$TARGETARCH
####################################################################################################
FROM docker.io/node:18 as numaflow-ui

WORKDIR /app

COPY ui/package.json ui/yarn.lock ui/

RUN JOBS=max yarn --cwd ui install --network-timeout 1000000

COPY ui ui
RUN NODE_OPTIONS="--max-old-space-size=2048" JOBS=max yarn --cwd ui build
RUN yarn --cwd ui build

####################################################################################################
# base
####################################################################################################
FROM alpine:3.12.3 as base
ARG ARCH
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/numaflow-linux-${ARCH} /bin/numaflow
RUN chmod +x /bin/numaflow

####################################################################################################
# numaflow
####################################################################################################
FROM scratch as numaflow
ARG ARCH
COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/numaflow /bin/numaflow
COPY --from=numaflow-ui /app/ui/build /ui/build
ENTRYPOINT [ "/bin/numaflow" ]

####################################################################################################
# testbase
####################################################################################################
FROM alpine:3.12.3 as testbase
ARG ARCH
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/e2eapi /bin/e2eapi
RUN chmod +x /bin/e2eapi

FROM scratch AS e2eapi
ARG ARCH
COPY --from=testbase /bin/e2eapi .
ENTRYPOINT ["/e2eapi"]
