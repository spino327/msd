
package com.k.msd.apps

import Console.{GREEN, RED, RESET, YELLOW_B, UNDERLINED}

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import scala.io.StdIn

class Repl {
  
  val processMap = scala.collection.mutable.HashMap[String, ReplApp]()
  
  val printMsg = (x:String) => Console.println(s"${RESET}${GREEN}$x${RESET}")
  val printPrompt = () => Console.print(s"${RESET}${GREEN}${UNDERLINED} > ${RESET}")
  val printErr = (x:String) => Console.println(s"${RESET}${RED}$x${RESET}") 

  def installOption(option: Tuple2[String, ReplApp]): Unit = {
    processMap += option
  }

  /**
   * Start REPL loop.
   */
  def loop(inputRDD:RDD[Tuple2[String, Map[String, Any]]]): Unit = {
    printMsg("Welcome to the REPL. Typed 'help' for options")

    var poisonPill = false
    while (poisonPill != true) {
      printPrompt()
      val option = StdIn.readLine()

      option match {
        case "help" => {
          printMsg("Help: ")
          processMap.foreach(x => Console.println(s"${RESET}${GREEN}" + x._1 + s" : ${RESET}" + x._2.help()))
          Console.println(s"${RESET}${GREEN}exit : ${RESET}exits the REPL")
        }
        case "exit" => { 
          printErr("Stopping REPL...")
          poisonPill = true
        }
        case _ => {
          if (processMap.contains(option)) {
            processMap(option).process(inputRDD)  
          } else
            printErr(s"Invalid option: $option")
        }
      }
    }
  }
}

class ReplApp(lambda:Function1[RDD[Tuple2[String, Map[String, Any]]], Unit], helpStr: String) {

  def help () : String = {
    return helpStr
  }

  def process (inputRDD:RDD[Tuple2[String, Map[String, Any]]]) : Unit = {
    lambda(inputRDD)
  } 
}
