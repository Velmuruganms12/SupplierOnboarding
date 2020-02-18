package com.onedot.udf

import org.apache.spark.sql.functions.udf
/**
 * Created by Velmurugan on 18/02/2020.
 *
 */
object HelperUDF {

  def getColors() = {

    udf((value: String) =>
      if (!(value.trim()).isEmpty() && (value.trim()) != null) {
        var color = "Other"
        if (value.indexOf(' ') >= 0) {
          color = value.split(" ").head
        } else color = value
        if (color.equalsIgnoreCase("silber")) {
          "silver"
        } else if (color.equalsIgnoreCase("violett")) {
          "violet"
        } else if (color.equalsIgnoreCase("weiß") || color.equalsIgnoreCase("weiss")) {
          "white"
        } else if (color.equalsIgnoreCase("rot")) {
          "red"
        } else if (color.equalsIgnoreCase("schwarz")) {
          "black"
        } else if (color.equalsIgnoreCase("grau")) {
          "grey"
        } else if (color.equalsIgnoreCase("anthrazit")) {
          "anthrazit"
        } else if (color.equalsIgnoreCase("blau")) {
          "blue"
        } else if (color.equalsIgnoreCase("blau")) {
          "blue"
        } else if (color.equalsIgnoreCase("braun")) {
          "brown"
        } else color
      } else {
        "Other"
      })
  }

}
