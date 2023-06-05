FROM balazsdukai/3dbag-sample-data:latest

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 775 /usr/local/bin/docker-entrypoint.sh && chown root:root /usr/local/bin/docker-entrypoint.sh
#ENV PATH $PATH:/usr/local/bin
#ENTRYPOINT ["docker-entrypoint.sh"]