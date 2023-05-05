package com.lfw.chapter03

import scala.util.matching.Regex

object Demo64MobileRegex {
  def main(args: Array[String]): Unit = {
    //构建正则表达式
    println(isMobileNumber("18801234567")) //true
    println(isMobileNumber("11111111111")) //false
  }

  def isMobileNumber(number: String): Boolean = {
    val regex: Regex = "^((13[0-9])|(14[5,7,9])|(15[^4])|(18[0-9])|(17[0,1,3,5,6,7,8]))[0-9]{8}$".r
    val length: Int = number.length
    regex.findFirstMatchIn(number.slice(length - 11, length)).isDefined
  }
}
