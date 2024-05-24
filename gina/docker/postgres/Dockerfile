FROM postgis/postgis:14-3.4 

MAINTAINER Gina Stavropoulou<g.stavropoulou@tudelft.nl>

# Set up environment variables
ENV PGDATA=/data
ENV POSTGRES_USER=baseregisters_test_user
ENV POSTGRES_PASSWORD=baseregisters_test_pswd
ENV POSTGRES_DB=baseregisters_test

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