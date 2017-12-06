
package com.k.msd

import java.io.File
import com.k.msd.input._
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.sys

object MsdApp {

  def file_2_KV (x: String, specificPaths: List[String]) : Tuple2[String, Map[String, Any]] = {
    val file = new File(x)

    if (file.exists()) {
      val current = HDF5Reader.process(x, specificPaths)

      val track_id = current[String]("/analysis/songs/track_id")
      val mapValues = Map[String, Any] (
        "/metadata/songs/artist_id" -> current[String]("/metadata/songs/artist_id"),
        "/metadata/songs/artist_name" -> current[String]("/metadata/songs/artist_name"),
        "/metadata/songs/title" -> current[String]("/metadata/songs/title"),
        "/metadata/songs/release" -> current[String]("/metadata/songs/release"),
        "/analysis/songs/track_id" -> current[String]("/analysis/songs/track_id"),
        "/metadata/songs/song_id" -> current[String]("/metadata/songs/song_id"),
        "/musicbrainz/songs/year" -> current[Int]("/musicbrainz/songs/year"),
        "/metadata/songs/artist_location" -> current[String]("/metadata/songs/artist_location"),
        "/analysis/songs/tempo" -> current[Double]("/analysis/songs/tempo")
      )
      return (track_id -> mapValues)
    }
    else
      return null
  }

  def main (args: Array[String]) {

    if (args.length < 3) {
      println("USAGE: MsdApp <hd5f_paths> <output_folder> <num_partitions>")
      sys.exit(-1)
    }

    val inputFile = args(0)
    val outputFile = args(1)
    val numPartitions = args(2).toInt
    val conf = new SparkConf().setAppName("MillionSongDataset Spark") 

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Load our input data.
    val file_paths =  sc.textFile(inputFile, numPartitions)
  
    // extracting data
    val specificPaths = List(
      "/metadata/songs/artist_id",    // Echo Nest ID: String
      "/metadata/songs/artist_name", 
      "/metadata/songs/title",         
      "/metadata/songs/release",      // album name from which the track was taken, some songs / tracks can come from many albums, we give only one
      "/analysis/songs/track_id",     // The Echo Nest ID of this particular track on which the analysis was done
      "/metadata/songs/song_id",      // The Echo Nest song ID, note that a song can be associated with many tracks (with very slight audio differences)
      "/musicbrainz/songs/year",      // year when this song was released, according to musicbrainz.org
      "/metadata/songs/artist_location",
      "/analysis/songs/tempo")

    // extracted data rdd
    val pairSongDataRDD = file_paths.map((x:String) => file_2_KV(x, specificPaths))
    // caching the rdd since we'll reuse it several times later
    pairSongDataRDD.cache()

    val numSongs = pairSongDataRDD.count()

    // how many songs don't have tempo
    val tempoRDD = pairSongDataRDD.filter({case (key, value) => value("/analysis/songs/tempo").asInstanceOf[Double] == 0.0})
    tempoRDD.cache()

    val noTempo = tempoRDD.count()
    println(tempoRDD.take(10).mkString("\n"))

    println(s"numSongs: $numSongs, noTempo: $noTempo")
    // pairSongDataRDD.saveAsTextFile(outputFile)

    tempoRDD.saveAsTextFile(outputFile)
  }
}

