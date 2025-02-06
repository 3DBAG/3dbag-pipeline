FROM postgis/postgis:14-3.4

LABEL org.opencontainers.image.authors="Gina Stavropoulou <gstavropoulou@tudelft.nl>, Balázs Dukai <balazs.dukai@3dgi.nl>"

ARG pg_user
ARG pg_pswd
ARG pg_db

# Set up environment variables
ENV POSTGRES_USER=$pg_user
ENV POSTGRES_PASSWORD=$pg_pswd
ENV POSTGRES_DB=$pg_db
ENV POSTGRES_HOST=localhost

ADD entrypoint.sh /docker-entrypoint-initdb.d/
RUN chmod 777 /docker-entrypoint-initdb.d/entrypoint.sh

# change work directory
WORKDIR /data

# Expose the PostgreSQL port
EXPOSE 5432

CMD ["postgres"]