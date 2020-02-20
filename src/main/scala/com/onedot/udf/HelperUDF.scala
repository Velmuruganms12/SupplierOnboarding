package com.onedot.udf

import com.onedot.common.Constants._
import org.apache.spark.sql.functions.udf

/**
 * Created by Velmurugan on 18/02/2020.
 * User Defined Function for Dataframe
 */
object HelperUDF{
/*
 Get English word from Key value store
 */
  def getColors(colors: Map[String, String]) = {
    udf((value: String) =>
      colors.getOrElse(value, OTHER)
    )
  }

  /*
Convert text to start with Captial Letter
 */
  def capitalize() = {
    udf((value: String) =>
      if(value.split(HYPHEN).size== 2){
        value.toLowerCase().split(HYPHEN).map(_.capitalize).mkString(HYPHEN)
      }else if(value.split(SPACE).size== 2){
        value.toLowerCase().split(SPACE).map(_.capitalize).mkString(SPACE)
      }else
      value.toLowerCase().capitalize
    )
  }


}
