package com.onedot.utils

import com.onedot.config.Context
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
/**
 * Created by Velmurugan on 18/02/2020.
 *
 */
object DataFrameUtils extends Context{


  def removeColumns(colsToRemove: Seq[String], df: DataFrame) =
    df.select(df.columns.filter(colName => !colsToRemove.contains(colName)).map(colName => new Column(colName)): _*)

  def selectColumns(colsToShow: Seq[String], df: DataFrame) =
    df.select(df.columns.filter(colName => colsToShow.contains(colName)).map(colName => new Column(colName)): _*)

  def orderColumns(df: DataFrame) = {
    val reorderedColumnNames: Array[String] =df.columns.sorted
    df.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
  }


  def writeDataframe(df: DataFrame, fileName:String)={
    df.show()
   df.repartition(1).write.option("header", "true").option("encoding", "UTF-8").csv("outputdata/"+fileName)
    println(" write completed "+ fileName+" count "+ df.count())
  }

  def readTargetExcel(): DataFrame ={
     sparkSession.read
      .format("com.crealytics.spark.excel")
      //.option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
      .option("useHeader", "true") // Required
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("inferSchema", "false") // Optional, default: false
      .option("addColorColumns", "true") // Optional, default: false
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      // .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      //.option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
      //.schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
      .load("inputdata/Target Data.xlsx")
  }

}
