package com.onedot.config

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

/**
 * Created by Velmurugan on 18/02/2020.
 *
 */
trait Context {

  lazy val sparkConf = new SparkConf().setAppName("Learn Spark").setMaster("local[*]").set("spark.cores.max", "2")
  lazy val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  sparkSession.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  sparkSession.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val startTime:DateTime= DateTime.now()
}
