package com.onedot.transform


import com.onedot.common.Constants._
import com.onedot.utils.{DataFrameUtils, Utilities}
import com.onedot.utils.DataFrameUtils.orderColumns
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object FeatureTransformation {

  //transform to target schema
  def matchTargetData(startTime:DateTime,spark: SparkSession, integratedDF: DataFrame): Unit = {

    //reading from Targetdata Excel
    val targetData = DataFrameUtils.readTargetExcel()
    targetData.show()
    targetData.createOrReplaceTempView("targetData")

    val distinctTargetData = spark.sql("select color as target_color, make as target_make, city as target_city from targetData").distinct()
    println(" after  distinctTargetData step " + Utilities.calculateRunTime(startTime, DateTime.now()))
    // Logic to identifying  Modal & Modal_variant using ML models


    // integratedDF Joined with Targetdata to get mismatch
    val widetable = integratedDF.join(distinctTargetData, lower(col("make")) === lower(col("target_make")) && lower(col("color")) === lower(col("target_color")) && lower(col("city")) === lower(col("target_city")), "left")
    import spark.sqlContext.implicits._
    val identifyRecordsDF = widetable.withColumn("targetData", when(length($"target_make") >= 1, lit("Match"))
      .otherwise(lit("MisMatch")))

    println(" after  identifyRecordsDF step " + Utilities.calculateRunTime(startTime, DateTime.now()))
    println("Total Records from SupplierFile " + identifyRecordsDF.count())
    DataFrameUtils.writeDataframe(orderColumns(identifyRecordsDF), "identify-supplierdata")
    println("Mismatched records Based City,Make,Color : " + identifyRecordsDF.filter(col("targetData") === "MisMatch").count())
    println(" after write  identifyRecordsDF step " + Utilities.calculateRunTime(startTime, DateTime.now()))
  }

}
