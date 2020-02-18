package com.onedot.utils

import org.apache.spark.sql.{Column, DataFrame}
/**
 * Created by Velmurugan on 18/02/2020.
 *
 */
object DataFrameUtils {


  def removeColumns(colsToRemove: Seq[String], df: DataFrame) = df.select(df.columns.filter(colName => !colsToRemove.contains(colName)).map(colName => new Column(colName)): _*)

  def selectColumns(colsToShow: Seq[String], df: DataFrame) = df.select(df.columns.filter(colName => colsToShow.contains(colName)).map(colName => new Column(colName)): _*)


  def writeDataframe(df: DataFrame, fileName:String)={
    df.show()
    println(" writeDataframe count "+ df.count())
    df.repartition(1).write.option("header", "true").option("encoding", "UTF-8").csv("outputdata/"+fileName)
  }

}
