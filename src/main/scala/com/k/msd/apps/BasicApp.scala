
package com.k.msd.apps

import com.k.msd.apps.ReplHelper._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

object BasicApp {

  def makeApp (output:String):ReplApp = {

    val lambda = (input:RDD[Tuple2[String, Map[String, Any]]]) => {

      // number of songs
      val numSongs = input.count()

      // individual artist_id
      val artistsRDD = input.map(kv => 
          kv._2("artist_id").asInstanceOf[String] -> kv._2("artist_name").asInstanceOf[String])
      artistsRDD.distinct().cache()
      val numArtist = artistsRDD.count()
      artistsRDD.saveAsTextFile(output + "/artists")
      artistsRDD.unpersist()

      // how many songs don't have tempo
      val noTempoRDD = input.filter({case (key, value) => value("tempo").asInstanceOf[Double] == 0.0})
      noTempoRDD.cache()
      val numNoTempo = noTempoRDD.count()
      noTempoRDD.saveAsTextFile(output + "/no_tempo")
      noTempoRDD.unpersist()

      // how many songs don't have year
      val noYearRDD = input.filter({case (key, value) => value("year").asInstanceOf[Int] == 0})
      noYearRDD.cache()
      val numNoYear = noYearRDD.count()
      noYearRDD.saveAsTextFile(output + "/no_year")
      noYearRDD.unpersist()

      // printing
      printMsg(s"numSongs: $numSongs, numArtist: $numArtist, numNoTempo: $numNoTempo, numNoYear: $numNoYear")
    }

    val help = "Computes number of songs, number of artist, and save the (artist_id, artist_name)"

    return new ReplApp(lambda, help)
  }

  }
