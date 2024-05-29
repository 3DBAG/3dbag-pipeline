FROM postgis/postgis:14-3.4 

MAINTAINER Gina Stavropoulou<g.stavropoulou@tudelft.nl>

ARG pg_user
ARG pg_pswd
ARG pg_db

ARG dagster_pg_user
ARG dagster_pg_pswd
ARG dagster_pg_db

# Set up environment variables
ENV PGDATA=/data

ENV POSTGRES_USER=$pg_user
ENV POSTGRES_PASSWORD=$pg_pswd
ENV POSTGRES_DB=$pg_db

ENV DAGSTER_PG_USER=$dagster_pg_user
ENV DAGSTER_PG_PASSWORD=$dagster_pg_pswd
ENV DAGSTER_PG_DB=$dagster_pg_db

# Create a directory to store PostgreSQL data and logs
RUN mkdir -p ${PGDATA} /tmp /var/log/postgresql 
RUN chmod a+rw ${PGDATA} /tmp /var/log/postgresql

ADD entrypoint.sh /docker-entrypoint-initdb.d/
RUN chmod 777 /docker-entrypoint-initdb.d/entrypoint.sh

# change work directory
WORKDIR /data

# Expose the PostgreSQL port
EXPOSE 5432

CMD ["postgres"]