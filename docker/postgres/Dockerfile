FROM postgis/postgis:14-3.4

LABEL org.opencontainers.image.authors="Gina Stavropoulou <gstavropoulou@tudelft.nl>, Balázs Dukai <balazs.dukai@3dgi.nl>"

ADD entrypoint.sh /docker-entrypoint-initdb.d/
RUN chmod 777 /docker-entrypoint-initdb.d/entrypoint.sh

# change work directory
WORKDIR /data

# Expose the PostgreSQL port
EXPOSE 5432

CMD ["postgres"]