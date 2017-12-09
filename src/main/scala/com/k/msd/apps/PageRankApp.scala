
package com.k.msd.apps

import com.k.msd.apps.ReplHelper._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

object PageRankApp {

  def makeApp (output:String):ReplApp = {

    val lambda = (input:RDD[Tuple2[String, Map[String, Any]]]) => {
    
      println("Something with PageRank")
    }

    val help = "Computes the pagerank of the artists"

    return new ReplApp(lambda, help)
  }

}
