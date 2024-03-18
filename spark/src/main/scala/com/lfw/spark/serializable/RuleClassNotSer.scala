package com.lfw.spark.serializable

/**
 * 使用一个 object 单例对象封装数据
 */
class RuleClassNotSer {
  val rulesMap: Map[String, String] = Map(
    "ln" -> "辽宁省",
    "sd" -> "山东省",
    "sh" -> "上海市",
    "bj" -> "北京市"
  )
}
