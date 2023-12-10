FROM openjdk:21 as builder

RUN --mount=type=bind,src=.,dst=/usr/src/trino-iris,rw \
    --mount=type=cache,target=/root/.m2 \
    cd /usr/src/trino-iris && \
    ./mvnw -DskipTests package && \
    mkdir /tmp/trino-iris && \
    tar -zxvf target/trino-iris-*-plugin.tar.gz -C /tmp/trino-iris/ --strip-components=1

FROM trinodb/trino:433

RUN --mount=type=bind,from=builder,src=/tmp/trino-iris,dst=/tmp/trino-iris \
    cp -R /tmp/trino-iris /usr/lib/trino/plugin/iris

