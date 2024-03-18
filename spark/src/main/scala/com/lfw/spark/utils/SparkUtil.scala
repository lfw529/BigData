package com.lfw.spark.utils

import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {
  def getContext(appName: String, isLocal: Boolean): SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    if (isLocal) {
      conf.setMaster("local[*]")
    }
    new SparkContext(conf)
  }
}
