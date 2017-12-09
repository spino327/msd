
package com.k.msd.input

import java.io.File
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/**
 * Preprocessor takes the hdf5 files and creates a pair RDD that can be saved 
 * either on memory or on disk for further processing
 */
object Preprocessor {

  // singleton variables
  private var paths:List[String] = null
  private var lambda:Function1[HDF5Obj, Map[String, Any]] = null

  /**
   * Sets the paths to extract from the input dataset
   */
  def setPaths(p: List[String]) : Unit = {
    paths = p 
  }

  /**
   * Sets the map builder function. e.g.
   * val mapFunc = (current:HDF5Obj) => Map[String, Any] (
   *     "/metadata/songs/artist_id" -> current[String]("/metadata/songs/artist_id"),
   *     "/metadata/songs/artist_name" -> current[String]("/metadata/songs/artist_name"),
   *     "/metadata/songs/title" -> current[String]("/metadata/songs/title"))
   */
  def setMapBuilder(f: Function1[HDF5Obj, Map[String, Any]]) : Unit = {
    lambda = f
  }

  /**
   * Extracts from an hdf5 file the required paths
   */
  def file_2_KV (x: String, specificPaths: List[String], lambdaFnc: Function1[HDF5Obj, Map[String, Any]]) : Tuple2[String, Map[String, Any]] = {
    val file = new File(x)

    var tuple: Tuple2[String, Map[String, Any]] = null

    if (file.exists()) {
      val current = HDF5Reader.process(x, specificPaths)

      val track_id = current[String]("/analysis/songs/track_id")
      val mapValues = lambdaFnc(current)
      
      tuple = (track_id -> mapValues)
    }

    return tuple
  }

  def makeRDD (input: RDD[String], sc: SparkContext): RDD[Tuple2[String, Map[String, Any]]] = {
   
    // since paths will be use in each executor, then we can send it as a broadcast variable
    val bpaths = sc.broadcast(paths)
    val blambda = sc.broadcast(lambda)

    // extracted data rdd
    val preprocessedRDD = input.map((x:String) => file_2_KV(x, bpaths.value, blambda.value))

    return preprocessedRDD
  }
}
