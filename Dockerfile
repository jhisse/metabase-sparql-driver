FROM clojure:temurin-21-tools-deps-trixie-slim AS builder-base

WORKDIR /app

COPY metabase/ ./metabase

COPY deps.edn Makefile ./
COPY resources/ ./resources
COPY src/ ./src

ENTRYPOINT ["make", "build"]

FROM builder-base AS builder

RUN make build

FROM metabase/metabase:v0.55.x

COPY --from=builder /app/target/sparql.metabase-driver.jar /plugins/