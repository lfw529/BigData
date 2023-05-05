package com.lfw.chapter03

object Demo19MapOperation {
  def main(args: Array[String]): Unit = {
    val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", "peru" -> "#CD*%#F")

    val nums: Map[Int, Int] = Map()

    println("colors 中的键为：" + colors.keys) //colors 中的键为：Set(red, azure, peru)
    println("colors 中的值为：" + colors.values) //colors 中的值为：MapLike.DefaultValuesIterable(#FF0000, #F0FFFF, #CD*%#F)
    println("检测 colors 是否为空：" + colors.isEmpty) //检测 colors 是否为空：false
    println("检测 nums 是否为空：" + nums.isEmpty) //检测 nums 是否为空：true
  }
}
