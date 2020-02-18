package com.onedot.utils

import org.joda.time.{DateTime, Seconds}

import scala.reflect.io.File

/**
 * Created by Velmurugan on 18/02/2020.
 *
 */
object Utilities {

  def calculateRunTime(startDate: DateTime, endDate: DateTime): String = Seconds.secondsBetween(startDate, endDate).toString().substring(2)


}
