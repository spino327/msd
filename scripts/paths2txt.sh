#!/usr/bin/env bash

# Flatten the paths of hdf5 files into a txt file
USAGE="./paths2txt.sh <folder_path> <file>"

if [ $# -lt 2 ]; then
  echo $USAGE
  exit -1
fi

INIT=$1
TARGET=$2

# traverse(prefix)
function traverse() {
  prefix=$1
  echo processing $prefix >&2
  if [ -f $prefix ]; then
    (( tmp = ${#prefix} - 3 ))
    if [ ${prefix:$tmp} == ".h5" ]; then
      echo $prefix
    fi
  else
    for element in `ls $prefix`; do
      traverse "$1/$element"
    done
  fi
}

# processing root folder
rm $TARGET
traverse $INIT >> $TARGET
