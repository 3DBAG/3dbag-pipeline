#!/bin/bash
# /data_container contains data that is distributed with the image, so the contents are
# copied to /data when a container is created. /data is bind-mounted to the host, which
# makes the contents of /data_container available to the host.
if [[ "$(ls -A /data_container)" ]] ; then cp -r /data_container/* /data/ ; fi