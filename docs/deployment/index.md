# Deployment

## Resources

The 3DBAG pipeline is a heavy process that requires a well configured database. Some instructions for configuring your database can be found here in the following links:

[Resource Consumption](https://www.postgresql.org/docs/10/runtime-config-resource.html)
[Write Ahead Log](https://www.postgresql.org/docs/12/runtime-config-wal.html)
[WAL Configuration](https://www.postgresql.org/docs/12/wal-configuration.html)

Indicatively, here are some specifications of our database setup:

```
shared_buffers = 24GB
max_parallel_workers = 24
max_connections = 150
effective_cache_size = 4GB
effective_io_concurrency = 100
maintenance_work_mem = 2GB
```