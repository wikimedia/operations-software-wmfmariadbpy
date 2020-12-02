FROM debian:buster
ARG DBDEPLOYER_VERSION=1.57.0
ARG MARIADB_SYS_VERSION=c000211d17f833f79e4fce9802d69834513079fb

RUN apt-get update -q -y && DEBIAN_FRONTEND=noninteractive apt-get install  --no-install-recommends -y \
    bash-completion \
    ca-certificates \
    curl \
    jq \
    less \
    libaio1 \
    libncurses5 \
    libnuma1 \
    libtinfo5 \
    lsof \
    ncurses-base \
    net-tools \
    procps \
    && apt-get clean
RUN curl -sSL https://github.com/datacharmer/dbdeployer/releases/download/v${DBDEPLOYER_VERSION}/dbdeployer-${DBDEPLOYER_VERSION}.linux.tar.gz | \
    tar -C /tmp -xzvf - && mv -v /tmp/dbdeployer-${DBDEPLOYER_VERSION}.linux /usr/local/bin/dbdeployer && dbdeployer --version
RUN mkdir /root/mariadb-sys && curl -sSL https://github.com/FromDual/mariadb-sys/archive/${MARIADB_SYS_VERSION}.tar.gz | tar -C /root/mariadb-sys -xzf - --strip-components=1

RUN mkdir /etc/bash_completion.d && printf 'alias ls="ls --color=auto"\n. /etc/bash_completion\n' >> /root/.bashrc
RUN dbdeployer init --skip-tarball-download && rm dbdeployer_completion.sh

COPY /contents/docker-entrypoint.sh /
COPY /contents/apply_sys_schema /usr/local/bin
ENTRYPOINT ["/docker-entrypoint.sh"]

VOLUME /cache
VOLUME /root/opt/mysql
WORKDIR /root
