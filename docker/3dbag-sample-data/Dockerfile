FROM postgis/postgis:14-3.3-novolume AS pg

ARG VERSION
LABEL org.opencontainers.image.authors="Balázs Dukai <balazs.dukai@3dgi.nl>"
LABEL org.opencontainers.image.vendor="3DGI"
LABEL org.opencontainers.image.title="3dbag-sample-data"
LABEL org.opencontainers.image.description="Sample data set for developing the 3D BAG"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.version=$VERSION

EXPOSE 5432
ENV POSTGRES_USER=db3dbag_user
ENV POSTGRES_DB=postgres
ENV POSTGRES_PASSWORD=db3dbag_1234


# Set up data directories
#   /data – This directory is to be bind-mounted on the host, so that the data that is
#       generated in a container will also be available to the host.
#   /data_container – Contains the data that is distributed with the image. Files from
#       this directory are copied to /data when a container is created, so that they
#       become available to the host too.
RUN mkdir /data /data_container
RUN chmod a+rw /data /data_container
ADD docker-entrypoint-copydata.sh /docker-entrypoint-initdb.d/
RUN chmod 777 /docker-entrypoint-initdb.d/docker-entrypoint-copydata.sh
# Need our adapted version of the docker-entrypoint.sh script that comes with the
# postgres image, to make sure that the data directory copy script is always run on the
# container startup.
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 775 /usr/local/bin/docker-entrypoint.sh && chown root:root /usr/local/bin/docker-entrypoint.sh

# Initialize the database with sample data, see https://registry.hub.docker.com/_/postgres/
ADD baseregisters.sh /docker-entrypoint-initdb.d/baseregisters.sh
RUN chmod 777 /docker-entrypoint-initdb.d/baseregisters.sh