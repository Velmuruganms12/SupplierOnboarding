package com.onedot.config

import com.onedot.common.Constants._
import com.onedot.utils.Utilities
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

/**
 * Created by Velmurugan on 18/02/2020.
 *
 */
trait Context{

  lazy val sparkConf = new SparkConf().setAppName("OnboardingApp").setMaster("local[*]").set("spark.cores.max", "2")
  lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  sparkSession.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", FALSE)
  sparkSession.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", FALSE)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val startTime:DateTime= DateTime.now()

  // Colors German & English - It can be generated once or read from database
  val colors=Utilities.getColorMap()
}
