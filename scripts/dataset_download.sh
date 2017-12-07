#!/usr/bin/env bash

# Downloads the dataset and unzip the data
echo "Dowloading the dataset..."
curl -C - -O http://static.echonest.com/millionsongsubset_full.tar.gz

echo "Extracting into the folder 'dataset'"
mkdir -p dataset
tar -xf millionsongsubset_full.tar.gz -C dataset
