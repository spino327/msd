
package com.k.msd

import java.io.File
import com.k.msd.input._
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.sys

object MsdApp {

  def main (args: Array[String]) {

    if (args.length < 3) {
      println("USAGE: MsdApp <hd5f_paths> <output_folder> <num_partitions>")
      sys.exit(-1)
    }

    val inputFile = args(0)
    val outputFile = args(1)
    val numPartitions = args(2).toInt
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[" + numPartitions + "]") //.setMaster("local[2]");

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Load our input data.
    val file_paths =  sc.textFile(inputFile, numPartitions)
  
    // extracting data
    val specificPaths = List("/analysis/songs/track_id", "/metadata/songs/artist_name", "/metadata/songs/artist_id", "/metadata/songs/title")

    val file_2_KV = (x: String) => {
      val file = new File(x)
     
      if (file.exists()) {
        val current = HDF5Reader.process(x, specificPaths)
        
        val track_id = current[String]("/analysis/songs/track_id")
        val mapValues = Map[String, Any] (
          "/metadata/songs/artist_name" -> current[String]("/metadata/songs/artist_name"),
          "/metadata/songs/artist_id" -> current[String]("/metadata/songs/artist_id"),
          "/metadata/songs/title" -> current[String]("/metadata/songs/title")
        )
        track_id -> mapValues
      }
      else
        None
    }

    val kvHDF5 = file_paths.map((x:String) => file_2_KV(x))

    kvHDF5.saveAsTextFile(outputFile)
    
    
    // Split up into words.
    // val words = input.flatMap(line => line.split(" "))
    // Transform into word and count. Note concise "underscore" notation for lambda
    // val counts = words.map(word => (word, 1)).reduceByKey( _ + _)
    // Save the word count back out to a text file, causing evaluation.
    // counts.saveAsTextFile(outputFile)
  }
}

