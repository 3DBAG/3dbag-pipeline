#!/bin/bash

while IFS=, read -r one; do
    ln -s /data/pointcloud/AHN4/disk/01_LAZ/C_${one^^}.LAZ /data/pointcloud/AHN4/ept_test/source/
  done < selected_ahn.csv