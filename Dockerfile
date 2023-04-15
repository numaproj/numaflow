ARG BASE_IMAGE=scratch
ARG ARCH=$TARGETARCH
####################################################################################################
# base
####################################################################################################
FROM alpine:3.17 as base
ARG ARCH
RUN apk update && apk upgrade && \
  apk add ca-certificates && \
  apk --no-cache add tzdata

COPY dist/numaflow-linux-${ARCH} /bin/numaflow
RUN adduser \
  -h "/dev/null" \
  -g "" \
  -s "/sbin/nologin" \
  -D \
  -H \
  -u 1000 \
  numaflow

RUN chmod +x /bin/numaflow

####################################################################################################
# numaflow
####################################################################################################
ARG BASE_IMAGE
FROM ${BASE_IMAGE} as numaflow
COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/numaflow /bin/numaflow
COPY --from=base /etc/passwd /etc/passwd
COPY ui/build /ui/build
USER numaflow
ENTRYPOINT [ "/bin/numaflow" ]

####################################################################################################
# testbase
####################################################################################################
FROM alpine:3.17 as testbase
RUN apk update && apk upgrade && \
  apk add ca-certificates && \
  apk --no-cache add tzdata
RUN adduser \
  -h "/dev/null" \
  -g "" \
  -s "/sbin/nologin" \
  -D \
  -H \
  -u 1000 \
  numaflow

COPY dist/e2eapi /bin/e2eapi
RUN chmod +x /bin/e2eapi

####################################################################################################
# testapi
####################################################################################################
FROM scratch AS e2eapi
COPY --from=testbase /bin/e2eapi .
COPY --from=testbase /etc/passwd /etc/passwd
USER numaflow
ENTRYPOINT ["/e2eapi"]
