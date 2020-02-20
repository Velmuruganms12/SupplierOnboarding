package com.onedot.utils

import com.onedot.common.Constants._
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Seconds}

import scala.io.Source

/**
 * Created by Velmurugan on 18/02/2020.
 *
 */
object Utilities extends App{

  /*
  Method to calculate Runtime in seconds
   */
  def calculateRunTime(startDate: DateTime, endDate: DateTime): String =
    Seconds.secondsBetween(startDate, endDate).toString().substring(2)

  def getColorMap(): Map[String, String] = {
    val bufferedSource = Source.fromFile("inputdata/colors.csv")
    var colors = Map[String, String]()
    for (line <- bufferedSource.getLines) {
      val cols = line.split(COMMA).map(_.trim)
      colors += (cols(0) -> cols(1))
    }
    bufferedSource.close
    colors
  }



  //getColorMap()
}
