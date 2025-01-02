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

# Default variable values
build_tyler=false
build_tyler_db=false
build_geoflow_roofer=false
build_geos=false
build_proj=false
build_lastools=false
build_gdal=false
build_geotiff=false
build_pdal=false

geos_version="3.12.1"
geotiff_version="1.7.3"
proj_version="9.4.0"
lastools_version="2.0.3"
gdal_version="3.8.5"
pdal_version="2.8.0"
geoflow_bundle_version="2024.08.09"

jobs=8
root_dir=$PWD
clean_up=false

# Function to display script usage
# Ref.: https://medium.com/@wujido20/handling-flags-in-bash-scripts-4b06b4d0ed04
usage() {
 echo "Usage: $0 [OPTIONS]"
 echo "Options:"
 echo " -h, --help                Display this help message"
 echo " -d, --dir                 Change to this directory"
 echo " --clean                   Delete all build directories, incl. vcpkg"
 echo " -j, --jobs                Number of jobs to run in parallel for the compilation [default 8]"
 echo " --build-all               Build all dependencies"
 echo " --build-tyler             Build Tyler"
 echo " --build-tyler-db          Build Tyler-db"
 echo " --build-geoflow-roofer    Build Geoflow-roofer"
 echo " --build-geos              Build GEOS"
 echo " --build-proj              Build PROJ"
 echo " --build-lastools          Build LASTools"
 echo " --build-gdal              Build GDAL"
 echo " --build-geotiff           Build GeoTIFF"
 echo " --build-pdal              Build PDAL"
}

has_argument() {
    [[ ("$1" == *=* && -n ${1#*=}) || ( ! -z "$2" && "$2" != -*)  ]];
}

extract_argument() {
  echo "${2:-${1#*=}}"
}

# Function to handle options and arguments
handle_options() {
  while [ $# -gt 0 ]; do
    case $1 in
      -h | --help)
        usage
        exit 0
        ;;
      -d | --dir*)
        if ! has_argument $@; then
          echo "Directory not specified." >&2
          usage
          exit 1
        fi
        root_dir=$(extract_argument $@)
        shift
        ;;
      --clean)
        clean_up=true
        ;;
      -j | --jobs*)
        if ! has_argument $@; then
          echo "Number of jobs not specified." >&2
          usage
          exit 1
        fi
        jobs=$(extract_argument $@)
        shift
        ;;
      --build-all)
        build_tyler=true
        build_tyler_db=true
        build_geoflow_roofer=true
        build_geos=true
        build_proj=true
        build_lastools=true
        build_gdal=true
        build_geotiff=true
        build_pdal=true
        ;;
      --build-tyler)
        build_tyler=true
        ;;
      --build-tyler-db)
        build_tyler_db=true
        ;;
      --build-geoflow-roofer)
        build_geoflow_roofer=true
        ;;
      --build-geos)
        build_geos=true
        ;;
      --build-proj)
        build_proj=true
        ;;
      --build-lastools)
        build_lastools=true
        ;;
      --build-gdal)
        build_gdal=true
        ;;
      --build-geotiff)
        build_geotiff=true
        ;;
      --build-pdal)
        build_pdal=true
        ;;
      *)
        echo "Invalid option: $1" >&2
        usage
        exit 1
        ;;
    esac
    shift
  done
}

# Main script execution
handle_options "$@"
cd $root_dir || exit

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
  wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/createGLB.json -O "$tyler_resources"/geof/createGLB.json
  wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/createMulti.json -O "$tyler_resources"/geof/createMulti.json
  wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/metadata.json -O "$tyler_resources"/geof/metadata.json
  wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/process_feature.json -O "$tyler_resources"/geof/process_feature.json
  wget --no-verbose https://raw.githubusercontent.com/3DGI/tyler/multi-format-output/resources/geof/process_feature_multi.json -O "$tyler_resources"/geof/process_feature_multi.json
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

if [ "$build_geos" = true ] ; then
  rm -rf geos-${geos_version}
  wget --no-verbose https://download.osgeo.org/geos/geos-${geos_version}.tar.bz2
  tar xfj geos-${geos_version}.tar.bz2
  mkdir geos-${geos_version}/build
  cmake \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$root_dir \
      -DBUILD_TESTING:bool=OFF \
      -DBUILD_DOCUMENTATION:bool=OFF \
      -DBUILD_SHARED_LIBS:bool=ON \
      -S geos-${geos_version} \
      -B geos-${geos_version}/build
  cmake --build geos-${geos_version}/build -j $jobs --target install --config Release

  rm -rf geos-${geos_version}
  rm geos-${geos_version}.tar.bz2
fi

if [ "$build_lastools" = true ] ; then
  printf "\n\nInstalling LAStools...\n\n"
  cd $root_dir || exit
  rm -rf LAStools
  wget --no-verbose https://github.com/LAStools/LAStools/archive/refs/tags/v${lastools_version}.zip -O LAStools.zip
  unzip -q LAStools.zip
  mkdir LAStools-${lastools_version}/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -S LAStools-${lastools_version} \
    -B LAStools-${lastools_version}/build
  cmake --build LAStools-${lastools_version}/build -j $jobs --target install --config Release

  rm -rf LAStools-${lastools_version}
  rm LAStools.zip
fi

if [ "$build_proj" = true ] ; then
  printf "\n\nInstalling proj...\n\n"
  cd $root_dir || exit
  rm -rf proj-${proj_version}
  wget --no-verbose https://download.osgeo.org/proj/proj-${proj_version}.tar.gz
  tar -xf proj-${proj_version}.tar.gz
  mkdir proj-${proj_version}/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -S proj-${proj_version} \
    -B proj-${proj_version}/build
  cmake --build proj-${proj_version}/build -j $jobs --target install --config Release

  rm -rf proj-${proj_version}
  rm proj-${proj_version}.tar.gz
fi

if [ "$build_geotiff" = true ] ; then
  printf "\n\nInstalling GeoTIFF...\n\n"
  cd $root_dir || exit
  rm -rf libgeotiff-${geotiff_version}
  wget --no-verbose https://github.com/OSGeo/libgeotiff/releases/download/${geotiff_version}/libgeotiff-${geotiff_version}.tar.gz
  tar -xf libgeotiff-${geotiff_version}.tar.gz
  mkdir libgeotiff-${geotiff_version}/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -DCMAKE_PREFIX_PATH=$root_dir \
    -S libgeotiff-${geotiff_version} \
    -B libgeotiff-${geotiff_version}/build
  cmake --build libgeotiff-${geotiff_version}/build -j $jobs --target install --config Release

  rm -rf libgeotiff-${geotiff_version}
  rm libgeotiff-${geotiff_version}.tar.gz
fi

if [ "$build_gdal" = true ] ; then
  printf "\n\nInstalling GDAL...\n\n"
  cd $root_dir || exit
  rm -rf gdal-${gdal_version}
  wget --no-verbose https://github.com/OSGeo/gdal/releases/download/v${gdal_version}/gdal-${gdal_version}.tar.gz
  tar -xf gdal-${gdal_version}.tar.gz
  mkdir gdal-${gdal_version}/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -DCMAKE_PREFIX_PATH=$root_dir \
    -S gdal-${gdal_version} \
    -B gdal-${gdal_version}/build
  cmake --build gdal-${gdal_version}/build -j $jobs --target install --config Release

  rm -rf gdal-${gdal_version}
  rm gdal-${gdal_version}.tar.gz
fi

if [ "$build_pdal" = true ] ; then
  printf "\n\nInstalling PDAL...\n\n"
  cd $root_dir || exit
  rm -rf PDAL-${pdal_version}-src
  wget --no-verbose https://github.com/PDAL/PDAL/releases/download/${pdal_version}/PDAL-${pdal_version}-src.tar.gz
  tar -xf PDAL-${pdal_version}-src.tar.gz
  mkdir PDAL-${pdal_version}-src/build
  cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -DCMAKE_PREFIX_PATH=$root_dir \
    -S PDAL-${pdal_version}-src \
    -B PDAL-${pdal_version}-src/build
  cmake --build PDAL-${pdal_version}-src/build -j $jobs --target install --config Release

  rm -rf PDAL-${pdal_version}-src
  rm PDAL-${pdal_version}-src.tar.gz
fi

if [ "$build_geoflow_roofer" = true ] ; then
  cd $root_dir || exit
  if ! [ -d vcpkg ] ; then
    printf "\n\nInstalling vcpkg...\n\n"
    git clone https://github.com/microsoft/vcpkg.git
    cd vcpkg && ./bootstrap-vcpkg.sh -disableMetrics
  fi
  export VCPKG_ROOT="$root_dir/vcpkg"

  printf "\n\nInstalling Geoflow-roofer...\n\n"
  cd $root_dir || exit
  git clone https://github.com/3DBAG/geoflow-roofer.git
  mkdir geoflow-roofer/build

  cd geoflow-roofer
  $root_dir/vcpkg/vcpkg x-update-baseline
  cd $root_dir

  cmake \
    --preset vcpkg-minimal \
    -DRF_USE_LOGGER_SPDLOG=ON \
    -DRF_BUILD_APPS=ON \
    -DCMAKE_INSTALL_PREFIX=$root_dir \
    -S geoflow-roofer \
    -B geoflow-roofer/build
  cmake --build geoflow-roofer/build -j $jobs --target install --config Release

  geoflow_flowcharts=share/geoflow-bundle/flowcharts
  if ! [ -d "$geoflow_flowcharts" ] ; then
    mkdir -p "$geoflow_flowcharts"
  fi
  wget --no-verbose https://raw.githubusercontent.com/geoflow3d/gfc-brecon/79ab70bc7b08aee37a1ceca7e3bb4db18c0f2778/stream/reconstruct_bag.json -O "$geoflow_flowcharts/reconstruct_bag.json"

  rm -rf geoflow-roofer
fi

if [ "$clean_up" = true ] ; then
  cd $root_dir || exit
  printf "\n\nDeleting build artifacts...\n\n"

  rm geos-${geos_version}.tar.bz2 || true
  rm -rf geos-${geos_version} || true
  rm LAStools.zip || true
  rm -rf LAStools-${lastools_version} || true
  rm proj-${proj_version}.tar.gz || true
  rm -rf proj-${proj_version} || true
  rm gdal-${gdal_version}.tar.gz || true
  rm -rf gdal-${gdal_version} || true
  rm gdal-${gdal_version}.tar.gz || true
  rm -rf gdal-${gdal_version} || true
  rm libgeotiff-${geotiff_version}.tar.gz || true
  rm -rf libgeotiff-${geotiff_version} || true
  rm PDAL-${pdal_version}-src.tar.gz || true
  rm -rf PDAL-${pdal_version}-src || true
  rm -rf build || true
  rm -rf geoflow-bundle-src || true
  rm -rf geoflow-roofer || true
  rm -rf vcpkg || true
fi
