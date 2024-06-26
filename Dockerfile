FROM postgres:16.2

#######################################################################
# Prepare ENVs
ENV PG_CRON_VERSION="1.6.2"
ENV PG_CRON_DB="task5"

#######################################################################
# Prepare the build requirements for the rdkit compilation:
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-server-dev-all postgresql-contrib \
    libcurl4-openssl-dev \
    wget jq cmake build-essential ca-certificates && \
# Install pg_cron:
    mkdir /build && \
    cd /build && \
    wget https://github.com/citusdata/pg_cron/archive/v$PG_CRON_VERSION.tar.gz && \
    tar xzvf v$PG_CRON_VERSION.tar.gz && \
    cd pg_cron-$PG_CRON_VERSION && \
    make && \
    make install && \
# Clean up:
    cd / && \
    rm -rf /build && \
    apt-get remove -y wget jq cmake build-essential ca-certificates && \
    apt-get autoremove --purge -y && \
    apt-get clean && \
    apt-get purge && \
    rm -rf /var/lib/apt/lists/*

RUN echo "shared_preload_libraries = 'task5'" >> /var/lib/postgresql/data/postgresql.conf
RUN echo "cron.database_name = '${PG_CRON_DB:-pg_cron}'" >> /var/lib/postgresql/data/postgresql.conf

COPY ./docker-entrypoint.sh /usr/local/bin/
COPY ./main/data.sql /docker-entrypoint-initdb.d/data.sql
RUN chmod a+x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]

EXPOSE 5432
CMD ["postgres"]