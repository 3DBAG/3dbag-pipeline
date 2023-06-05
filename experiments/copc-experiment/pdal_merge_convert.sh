#!/bin/bash

parallel pdal pipeline --stream {} ::: /home/bdukai/software/copc-experiment/pdal_merge_convert_tmp/*.json

pdal pipeline --stream /home/bdukai/software/copc-experiment/pdal_merge_tmp.json

