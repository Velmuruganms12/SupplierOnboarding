package com.onedot

import com.onedot.config.Context
import com.onedot.transform.FeatureTransformation
import com.onedot.udf.HelperUDF
import com.onedot.utils.{DataFrameUtils, Utilities}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import com.onedot.common.Constants._
import com.onedot.utils.DataFrameUtils.orderColumns
/**
 * Created by Velmurugan on 18/02/2020.
 * - [[OnboardingPipelineApp]] is the main entry point  to start the Onboarding application.
 */
object OnboardingPipelineApp extends App with Context {


  // Input Data
  println("starting application  " + Utilities.calculateRunTime(startTime, DateTime.now()))
  val supplierInputdata = sparkSession.read.option("inferSchema", TRUE).json("inputdata/supplier_car.json")


  //Pre-Processing
  //To get the target data, Inputdata Attributes are converted from Row to Columns
  val preProcessedDF = supplierInputdata.groupBy("ID", "MakeText", "ModelText").pivot("Attribute Names").agg(first("Attribute Values"))
  preProcessedDF.cache()
  DataFrameUtils.writeDataframe(orderColumns(preProcessedDF), "preprocssed-supplierdata")
  println("after  Pre-Processing Step " + Utilities.calculateRunTime(startTime, DateTime.now()))


  //Normalisation

  //Color translated to English and matched with target data.
  //Make Matched with target data.
  import sparkSession.sqlContext.implicits._
  val normalisedDf = preProcessedDF.withColumn("color_tmp", HelperUDF.getColors(colors)(when(size(split($"BodyColorText", SPACE)) === 2, split($"BodyColorText", SPACE)(0))
    .otherwise(col("BodyColorText"))))
    .withColumn("make_tmp", HelperUDF.capitalize()($"MakeText")).drop("BodyColorText").drop("MakeText")
    .withColumnRenamed("color_tmp", "BodyColorText")
    .withColumnRenamed("make_tmp", "MakeText")
  DataFrameUtils.writeDataframe(orderColumns(normalisedDf), "normalised-supplierdata")
  println("after  Normalization Step " + Utilities.calculateRunTime(startTime, DateTime.now()))


  //Extraction
  //Unit and value extracted from ComsumptionTotalText
  val extractedDf = normalisedDf.withColumn("_tmp", split($"ConsumptionTotalText", SPACE))
    .withColumn("extracted-value-ConsumptionTotalText", $"_tmp".getItem(0))
    .withColumn("extracted-unit-ConsumptionTotalText", $"_tmp".getItem(1))
    .drop("_tmp")
  DataFrameUtils.writeDataframe(extractedDf, "extracted-supplierdata")
  println("after  Extraction Step " + Utilities.calculateRunTime(startTime, DateTime.now()))

  //Integration
  //Rename exist column & creating new column to match Target Schema
  val df = extractedDf
    .withColumnRenamed("BodyColorText", "color")
    .withColumnRenamed("MakeText", "make")
    .withColumnRenamed("City", "city")
    .withColumnRenamed("ModelText", "modal")
    .withColumnRenamed("BodyTypeText", "modal_variant")
    .withColumn("mileage", lit(0))
    .withColumn("mileage_unit", lit(""))
    .withColumn("price_on_request", lower(lit(false)))
    .withColumn("type", lit("car"))
    .withColumn("zip", lit(""))
    .withColumn("manufacture_month", lit(""))
    .withColumn("manufacture_year", lit(""))
    .withColumn("fuel_consumption_unit", lit(""))

  //Selecting Specific Column to Match Target Schema
  val integratedDF = DataFrameUtils.selectColumns(List("carType", "color", "condition", "currency", "drive",
    "city", "country", "make", "manufacture_year", "mileage", "mileage_unit", "modal", "modal_variant", "price_on_request", "type", "zip", "manufacture_month", "fuel_consumption_unit"), df)

  DataFrameUtils.writeDataframe(orderColumns(integratedDF), "Integrated-supplierdata")
  println(" after  Integration step " + Utilities.calculateRunTime(startTime, DateTime.now()))


  //Product Matching - Enrich or Add new
  //FeatureTransformation.matchTargetData(startTime,sparkSession,integratedDF)

}
