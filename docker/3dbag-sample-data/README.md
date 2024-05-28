# Sample data for developing the 3D BAG

The purpose of this data set is to use it for developing and testing the 3D BAG 
pipeline. It contains the source, intermediary and output data for producing the 3D BAG 
for a small area.
It is generated from the same pipeline as the complete 3D BAG, and it is updated 
automatically with the latest 3D BAG data.

The sample data is distributed with the image in files in `/data_container`, `/data` 
(see below), and in a database `baseregisters`.

[DockerHub repository](https://hub.docker.com/r/balazsdukai/3dbag-sample-data)

## Versioning

The latest image is always tagged as `latest`.
There is also an explicit version that corresponds to `latest`.
The version is determined from the run of the job that generated the sample data (and not the job that created the image). 
It follows the pattern of `'date of the run'-'run ID'`, e.g. `2022-10-05-7e37d6e0`.

## Install

```sh
$ docker pull balazsdukai/3dbag-sample-data:latest
```

## Database credentials

+ username: db3dbag_user
+ password: db3dbag_1234

## Usage

Create a container with the name, e.g. `db3dbag`.
If you bind-mount the `/data` dir on the host, make sure that the permissions are set 
up correctly on the host dir. See below.

```sh
$ docker create \
  --name db3dbag \
  -p <any local port>:5432 \
  -v <any local dir>:/data \
  balazsdukai/3dbag-sample-data:latest
```

Start the container.

```sh
$ docker start db3dbag
```

Access the database.

```sh
$ psql -p <any local port> -U db3dbag_user -d baseregisters -h localhost
```

Don't forget to remove the container when you are done.

## Data files

`/data_container` contains data that is distributed with the image and the contents are
copied to `/data` when a container is created. 
`/data` is meant to be bind-mounted on the host, thus making the contents of 
`/data_container` available to the host.

The `/data` container directory needs to be 
[bind-mounted](https://docs.docker.com/storage/bind-mounts/) 
on the `'any local dir'`, therefore `'any local dir'` must have a permission
mode of 777 (`chmod -R a+rwx 'any local dir'`).

It is advised to use a temporary directory as the `'any local dir'`.

## Updating the image

The image is meant to be used for ephemeral containers that are used for testing and 
then discarded. It is not meant to be updated manually.

The image update happens through some jobs in the 3dbag-pipeline. 
These jobs generate the sample area, update the image with the new sample data and push the image to DockerHub.
Therefore, in order to update the image, the corresponding jobs need to be updated and run.
