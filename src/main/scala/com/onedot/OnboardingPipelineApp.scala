package com.onedot

import com.onedot.config.Context
import com.onedot.udf.HelperUDF
import com.onedot.utils.{DataFrameUtils, Utilities}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

/**
 * Created by Velmurugan on 18/02/2020.
 * - [[OnboardingPipelineApp]] is the main entry point  to start the Onboarding application.
 */
object OnboardingPipelineApp extends App with Context {


  // Input Data
  println("starting application  " + Utilities.calculateRunTime(startTime, DateTime.now()))
  val supplierInputdata = sparkSession.read.option("inferSchema", true).json("inputdata/supplier_car.json")

  //Pre-Processing
  val preProcessedDF = supplierInputdata.groupBy("ID", "MakeText", "ModelText").pivot("Attribute Names").agg(first("Attribute Values"))
  preProcessedDF.cache()
  DataFrameUtils.writeDataframe(preProcessedDF,"preprocssed-supplierdata")
  println("after  Pre-Processing " + Utilities.calculateRunTime(startTime, DateTime.now()))


  //Normalisation
  import sparkSession.sqlContext.implicits._
  val normalisedDf = preProcessedDF.withColumn("color", HelperUDF.getColors()(preProcessedDF.col("BodyColorText"))).drop("Properties")
  DataFrameUtils.writeDataframe(normalisedDf, "normalised-supplierdata")
  println("after  Normalization " + Utilities.calculateRunTime(startTime, DateTime.now()))


  //Extraction
  val extractedDf=normalisedDf.withColumn("_tmp", split($"ConsumptionTotalText", " "))
    .withColumn("extracted-value-ConsumptionTotalText", $"_tmp".getItem(0))
    .withColumn("extracted-unit-ConsumptionTotalText", $"_tmp".getItem(1))
    .drop("_tmp")
  DataFrameUtils.writeDataframe(extractedDf, "extracted-supplierdata")

  //Integration
  val df=extractedDf
    .withColumnRenamed("MakeText", "make")
    .withColumnRenamed("ModelText", "model")
    .withColumnRenamed("BodyTypeText", "modal_variant")
    .withColumnRenamed("City", "city")
    .withColumn("carType", lit(""))
    .withColumn("condition", lit(""))
    .withColumn("currency", lit(""))
    .withColumn("drive", lit(""))
    .withColumn("country", lit(""))
    .withColumn("mileage", lit(0))
    .withColumn("mileage_unit", lit(""))
    .withColumn("price_on_request", lit(false))
    .withColumn("type", lit("car"))
    .withColumn("zip", lit(""))
    .withColumn("manufacture_month", lit(""))
    .withColumn("manufacture_year", lit(""))
    .withColumn("fuel_consumption_unit", lit(""))

  val IntegratedDF=DataFrameUtils.selectColumns(List("carType","color","condition","currency","drive",
    "city","country","make","manufacture_year","mileage","mileage_unit","model","model_variant","price_on_request","type","zip","manufacture_month","fuel_consumption_unit"),df)

  DataFrameUtils.writeDataframe(IntegratedDF, "Integrated-supplierdata")
  println("end of application after  Integration " + Utilities.calculateRunTime(startTime, DateTime.now()))


}
