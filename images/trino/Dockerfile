FROM --platform=$BUILDPLATFORM alpine/curl:latest AS plugin-provider

WORKDIR /app

RUN apk add --no-cache bash

COPY --chmod=755 ./get-plugin.sh get-plugin.sh

RUN ./get-plugin.sh

FROM trinodb/trino:476 AS base

COPY --from=plugin-provider /usr/lib/trino/plugin/hudi/ /usr/lib/trino/plugin/hudi/
