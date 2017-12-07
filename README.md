# Million Song Dataset


## Requirements
- maven 3.5  
- Java 8  
- scala 2.12
- Apache Spark 2.2.0

If you use brew.sh in your mac. You can install them using:  
> $ brew install maven; brew install apache-spark

## Setup

1. Downloading and preparing the data set (subset of Million Song dataset). Use the provided bash script as follows:
> $ ./scripts/dataset_download.sh

2. In order to prepare the input data for use it with Apache Spark. You can run the `paths2txt.sh` script that creates a .txt file with the paths of the HDF5 files `./scripts/paths2txt.sh /path/to/dataset/folder /path/to/output.txt`.  For instance, by default you can use:  
> $ ./scripts/paths2txt.sh dataset/MillionSongSubset/data paths.txt  
The `paths.txt` file will have the paths of each hdf5 file.

3. You can decode a particular hdf5 using the script `./scripts/characterize.sh` which runs an scala application that decodes the file. By default the script will decode the hdf5 file located at `src/test/resources/sample.h5`. For decoding another file you can pass the path as argument:    
> $ ./scripts/characterize.sh dataset/MillionSongSubset/data/A/B/A/TRABACN128F425B784.h5

## Processing the data.

1. You need to build the project using maven. 
> $ mvn package

2. Launch the spark job. You should use spark-submit in order to run the application. The entry point is the singleton `com.k.msd.MsdApp` which accepts 3 arguments `<hd5f_paths> <output_folder> <num_partitions>`. For instance, running spark locally with 4 executors, using 4 partitions for the input data:    
> $ /path/to/spark-submit --master local[4] --jars libs/sis-jhdf5-batteries_included.jar --class com.k.msd.MsdApp target/uber-msd-0.1.0.jar ./paths.txt ./output 4

Notice that you need to pass the argument `--jars libs/sis-jhdf5-batteries_included.jar` since the hdf5 library is not on the maven repository thus it is included with scope="system" in the pom.xml.
