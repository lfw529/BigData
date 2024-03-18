package com.lfw.spark.utils

import java.text.SimpleDateFormat

class DateUtilClass {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parse(str: String): Long = {
    //2022-05-23 11:39:30
    sdf.parse(str).getTime
  }
}
