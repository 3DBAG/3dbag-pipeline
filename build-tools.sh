#!/bin/bash

# Requirements:
# - C and C++ compilers (min. GCC 13 or Clang 18)
# - CMake
# - Rust toolchain
# - Git
# - wget
# - libgeos
# - sqlite3
# - libtiff

build_tyler=true
build_tyler_db=true
build_geoflow_roofer=true
build_geos=true
build_proj=true
build_lastools=true
build_gdal=true
build_geotiff=true
build_pdal=true

geos_version="3.12.1"
geotiff_version="1.7.3"
proj_version="9.4.0"
lastools_version="2.0.3"
gdal_version="3.8.5"
pdal_version="2.8.0"

jobs=8
root_dir=$PWD
clean_up=true


if [ "$build_tyler" = true ] ; then
  printf "\n\nInstalling Tyler...\n\n"
  cd $root_dir || exit
  cargo install \
    --root . \
    --git https://github.com/3DGI/tyler.git \
    --branch multi-format-output \
    --bin tyler

  tyler_resources=share/tyler/resources
  if ! [ -d "$tyler_resources" ] ; then
    mkdir -p "$tyler_resources"/geof
  fi
  wget https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/createGLB.json -O "$tyler_resources"/geof/createGLB.json
  wget https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/createMulti.json -O "$tyler_resources"/geof/createMulti.json
  wget https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/metadata.json -O "$tyler_resources"/geof/metadata.json
  wget https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/process_feature.json -O "$tyler_resources"/geof/process_feature.json
  wget https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/process_feature_multi.json -O "$tyler_resources"/geof/process_feature_multi.json
fi

if [ "$build_tyler_db" = true ] ; then
  printf "\n\nInstalling Tyler-db...\n\n"
  cd $root_dir || exit
  cargo install \
    --root . \
    --git https://github.com/3DGI/tyler.git \
    --branch postgres-footprints \
    --bin tyler-db
fi

if [ "$build_geoflow_roofer" = true ] ; then
  printf "\n\nInstalling vcpkg...\n\n"
  cd $root_dir || exit
  git clone https://github.com/microsoft/vcpkg.git
  cd vcpkg && ./bootstrap-vcpkg.sh
  export VCPKG_ROOT="$root_dir/vcpkg"

  printf "\n\nInstalling Geoflow-roofer...\n\n"
  cd $root_dir || exit
  git clone https://github.com/3DBAG/geoflow-roofer.git
  mkdir geoflow-roofer/build
  cmake \
    --preset vcpkg-minimal \
    -DRF_USE_LOGGER_SPDLOG=ON \
    -DRF_BUILD_APPS=ON \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -S geoflow-roofer \
    -B geoflow-roofer/build
  cmake --build geoflow-roofer/build -j $jobs --target install --config Release
  if [ "$clean_up" = true ] ; then
    rm -rf geoflow-roofer
    rm -rf vcpkg
  fi

  roofer_flowcharts=share/geoflow-roofer/flowcharts
  if ! [ -d "$roofer_flowcharts" ] ; then
    mkdir -p "$roofer_flowcharts"
  fi
  wget https://raw.githubusercontent.com/geoflow3d/gfc-brecon/79ab70bc7b08aee37a1ceca7e3bb4db18c0f2778/stream/reconstruct_bag.json -O "$roofer_flowcharts/reconstruct_bag.json"
fi

if [ "$build_geos" = true ] ; then
  rm -rf geos-${geos_version}
  wget https://download.osgeo.org/geos/geos-${geos_version}.tar.bz2
  tar xvfj geos-${geos_version}.tar.bz2
  mkdir geos-${geos_version}/build
  $cmake \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$root_dir \
      -DBUILD_TESTING:bool=OFF \
      -DBUILD_DOCUMENTATION:bool=OFF \
      -DBUILD_SHARED_LIBS:bool=ON \
      -S geos-${geos_version} \
      -B geos-${geos_version}/build
  $cmake --build geos-${geos_version}/build -j $jobs --target install --config Release
  if [ "$clean_up" = true ] ; then
    rm geos-${geos_version}.tar.bz2
    rm -rf geos-${geos_version}
  fi
fi

if [ "$build_lastools" = true ] ; then
  printf "\n\nInstalling LAStools...\n\n"
  cd $root_dir || exit
  rm -rf LAStools
  wget https://github.com/LAStools/LAStools/archive/refs/tags/v${lastools_version}.zip -O LAStools.zip
  unzip LAStools.zip
  mkdir LAStools-${lastools_version}/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -S LAStools-${lastools_version} \
    -B LAStools-${lastools_version}/build
  cmake --build LAStools-${lastools_version}/build -j $jobs --target install --config Release
  if [ "$clean_up" = true ] ; then
    rm LAStools.zip
    rm -rf LAStools-${lastools_version}
  fi
fi

if [ "$build_proj" = true ] ; then
  printf "\n\nInstalling proj...\n\n"
  cd $root_dir || exit
  rm -rf proj-${proj_version}
  wget https://download.osgeo.org/proj/proj-${proj_version}.tar.gz
  tar -xvf proj-${proj_version}.tar.gz
  mkdir proj-${proj_version}/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -S proj-${proj_version} \
    -B proj-${proj_version}/build
  cmake --build proj-${proj_version}/build -j $jobs --target install --config Release
  if [ "$clean_up" = true ] ; then
    rm proj-${proj_version}.tar.gz
    rm -rf proj-${proj_version}
  fi
fi

if [ "$build_gdal" = true ] ; then
  printf "\n\nInstalling GDAL...\n\n"
  cd $root_dir || exit
  rm -rf gdal-${gdal_version}
  wget https://github.com/OSGeo/gdal/releases/download/v${gdal_version}/gdal-${gdal_version}.tar.gz
  tar -xvf gdal-${gdal_version}.tar.gz
  mkdir gdal-${gdal_version}/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -DCMAKE_PREFIX_PATH=$root_dir \
    -S gdal-${gdal_version} \
    -B gdal-${gdal_version}/build
  cmake --build gdal-${gdal_version}/build -j $jobs --target install --config Release
  if [ "$clean_up" = true ] ; then
    rm gdal-${gdal_version}.tar.gz
    rm -rf gdal-${gdal_version}
  fi
fi

if [ "$build_geotiff" = true ] ; then
  printf "\n\nInstalling GeoTIFF...\n\n"
  cd $root_dir || exit
  rm -rf libgeotiff-${geotiff_version}
  wget https://github.com/OSGeo/libgeotiff/releases/download/${geotiff_version}/libgeotiff-${geotiff_version}.tar.gz
  tar -xvf libgeotiff-${geotiff_version}.tar.gz
  mkdir libgeotiff-${geotiff_version}/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -DCMAKE_PREFIX_PATH=$root_dir \
    -S libgeotiff-${geotiff_version} \
    -B libgeotiff-${geotiff_version}/build
  cmake --build libgeotiff-${geotiff_version}/build -j $jobs --target install --config Release
  if [ "$clean_up" = true ] ; then
    rm libgeotiff-${geotiff_version}.tar.gz
    rm -rf libgeotiff-${geotiff_version}
  fi
fi

if [ "$build_pdal" = true ] ; then
  printf "\n\nInstalling PDAL...\n\n"
  cd $root_dir || exit
  rm -rf PDAL-${pdal_version}-src
  wget https://github.com/PDAL/PDAL/releases/download/${pdal_version}/PDAL-${pdal_version}-src.tar.gz
  tar -xvf PDAL-${pdal_version}-src.tar.gz
  mkdir PDAL-${pdal_version}-src/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -DCMAKE_PREFIX_PATH=$root_dir \
    -S PDAL-${pdal_version}-src \
    -B PDAL-${pdal_version}-src/build
  cmake --build PDAL-${pdal_version}-src/build -j $jobs --target install --config Release

  if [ "$clean_up" = true ] ; then
    rm PDAL-${pdal_version}-src.tar.gz
    rm -rf PDAL-${pdal_version}-src
  fi
fi

if [ "$clean_up" = true ] ; then
  cd $root_dir || exit
  rm -rf build
fi
