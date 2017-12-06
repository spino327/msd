#!/usr/bin/env bash

USAGE="You can characterize a file by passing the path as argument. E.g ./characterize.sh <file_path (optional)>"

FILE=$1
BASE="`dirname $0`/.."
if [ -z $FILE ]; then
  echo $USAGE
  FILE="$BASE/src/test/resources/sample.h5"
  echo "Using default file : 'src/test/resources/sample.h5'"
fi

echo "Checking what is inside of an hdf5 file: '$FILE'"
scalac -classpath $BASE/libs/sis-jhdf5-batteries_included.jar $BASE/src/main/scala/BrowseHDF5.scala -d $BASE/scripts/
scala -classpath $BASE/libs/sis-jhdf5-batteries_included.jar:$BASE/scripts App $FILE
