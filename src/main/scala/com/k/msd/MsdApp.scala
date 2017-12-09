
package com.k.msd

import com.k.msd.input._
import com.k.msd.apps._

import java.io.File

import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.sys
import scala.io.StdIn
import Console.{GREEN, RED, RESET, YELLOW_B, UNDERLINED}

object MsdApp {

  private val repl = new Repl()
  repl.installOption("pagerank" -> new ReplApp((x) => {
    println("hahahahah: " + x.count())
  }, "pagerank options"))

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
      "/analysis/songs/tempo",
      "/metadata/similar_artists")

    // extracted data rdd
    // Load our input data.
    val file_paths =  sc.textFile(inputFile, numPartitions)

    Preprocessor.setPaths(specificPaths)
    Preprocessor.setMapBuilder((h5:HDF5Obj) => Map[String, Any] (
        "artist_id" -> h5[String]("/metadata/songs/artist_id"),
        "artist_name" -> h5[String]("/metadata/songs/artist_name"),
        "title" -> h5[String]("/metadata/songs/title"),
        "release" -> h5[String]("/metadata/songs/release"),
        "track_id" -> h5[String]("/analysis/songs/track_id"),
        "song_id" -> h5[String]("/metadata/songs/song_id"),
        "year" -> h5[Int]("/musicbrainz/songs/year"),
        "artist_location" -> h5[String]("/metadata/songs/artist_location"),
        "tempo" -> h5[Double]("/analysis/songs/tempo"),
        "similar_artists" -> h5[Array[String]]("/metadata/similar_artists")
      ))

    val pairSongDataRDD = Preprocessor.makeRDD(file_paths, sc)
    // caching the rdd since we'll reuse it several times later
    pairSongDataRDD.cache()

    // #####################
    // # REPL
    // #####################  
    // val printMsg = (x:String) => Console.println(s"${RESET}${GREEN}$x${RESET}")
    // val printPrompt = () => Console.print(s"${RESET}${GREEN}${UNDERLINED} > ${RESET}")
    // val printErr = (x:String) => Console.println(s"${RESET}${RED}$x${RESET}") 

    // printMsg("Welcome to the REPL. Typed 'help' for options")

    // var poisonPill = false
    // while (poisonPill != true) {
     
    //   printPrompt()
    //   val option = StdIn.readLine()
    
    //   option match {
    //     case "help" => println("Go to the doctor")
    //     case "exit" => { printErr("killing"); poisonPill = true}
    //     case _ => printErr(s"No recoignized: $option")
    //   }
    // }
    repl.loop(pairSongDataRDD)

    sys.exit(-1)

    val numSongs = pairSongDataRDD.count()

    // how many songs don't have tempo
    val tempoRDD = pairSongDataRDD.filter({case (key, value) => value("/analysis/songs/tempo").asInstanceOf[Double] == 0.0})
    tempoRDD.cache()

    val noTempo = tempoRDD.count()
    println(tempoRDD.take(10).mkString("\n"))

    println(s"numSongs: $numSongs, noTempo: $noTempo")
    // pairSongDataRDD.saveAsTextFile(outputFile)

    tempoRDD.saveAsTextFile(outputFile)


    val energyRDD = pairSongDataRDD.filter({case (key, value) => value("/analysis/songs/tempo").asInstanceOf[Double] == 0.0})
  }
}

