#!/bin/bash

# Test if all the required tools where successfully installed with tools-build.sh and
# they are available for the 3dbag-pipeline.

usage() {
 echo "Test if all the required tools where successfully installed with tools-build.sh and they are available for the 3dbag-pipeline."
 echo ""
 echo "Usage: $0 [OPTIONS]"
 echo "Options:"
 echo " -h, --help                Display this help message"
 echo " -d, --dir                 Directory where the tools are installed"
}

has_argument() {
  [[ ("$1" == *=* && -n ${1#*=}) || ( ! -z "$2" && "$2" != -*)  ]];
}

extract_argument() {
  echo "${2:-${1#*=}}";
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

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

all_ok=0

check_exe() {
  bin/$1 --version &> /dev/null
  if [ $? -eq 0 ] ; then
    printf "[${GREEN}OK${NC}].....$1\n"
  else
    printf "[${RED}FAIL${NC}]...$1\n"
    all_ok=1
  fi
}

check_exe_lastools() {
  bin/$1 -version &> /dev/null
  if [ $? -eq 0 ] ; then
    printf "[${GREEN}OK${NC}].....$1\n"
  else
    printf "[${RED}FAIL${NC}]...$1\n"
    all_ok=1
  fi
}

check_exe_help() {
  bin/$1 --help &> /dev/null
  if [ $? -eq 0 ] ; then
    printf "[${GREEN}OK${NC}].....$1\n"
  else
    printf "[${RED}FAIL${NC}]...$1\n"
    all_ok=1
  fi
}

check_exe "tyler"
check_exe "tyler-db"
check_exe "roofer"
check_exe "geof"
check_exe "ogr2ogr"
check_exe "ogrinfo"
check_exe "pdal"
check_exe "val3dity"
check_exe "cjval"
check_exe_help "sozip"
check_exe_lastools "las2las64"
check_exe_lastools "lasindex64"

exit $all_ok
