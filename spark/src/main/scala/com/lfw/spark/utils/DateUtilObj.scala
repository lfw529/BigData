package com.lfw.spark.utils

import java.text.SimpleDateFormat

object DateUtilObj {

  //多个 Task 使用了一个共享的 SimpleDateFormat，SimpleDateFormat 是线程不安全
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //线程安全的
  //val sdf: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(str: String): Long = {
    //2022-05-23 11:39:30
    sdf.parse(str).getTime
  }
}
