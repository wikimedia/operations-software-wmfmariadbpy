FROM debian:buster
ARG DBDEPLOYER_VERSION=1.56.0

RUN apt-get update -q -y && DEBIAN_FRONTEND=noninteractive apt-get install  --no-install-recommends -y \
    bash-completion \
    ca-certificates \
    curl \
    libaio1 \
    libncurses5 \
    libnuma1 \
    libtinfo5 \
    procps \
    && apt-get clean
RUN curl -sSL https://github.com/datacharmer/dbdeployer/releases/download/v${DBDEPLOYER_VERSION}/dbdeployer-${DBDEPLOYER_VERSION}.linux.tar.gz | \
    tar -C /tmp -xzvf - && mv -v /tmp/dbdeployer-${DBDEPLOYER_VERSION}.linux /usr/local/bin/dbdeployer && dbdeployer --version

RUN mkdir /etc/bash_completion.d && echo '. /etc/bash_completion' >> /root/.bashrc
RUN dbdeployer init --skip-tarball-download && rm dbdeployer_completion.sh

COPY /contents/docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
