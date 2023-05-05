package com.lfw.chapter03

object Demo20MapConcat {
  def main(args: Array[String]): Unit = {
    val colors1 = Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")

    val colors2 = Map("blue" -> "#0033FF",
      "yellow" -> "#FFFF00",
      "red" -> "#FF0000")

    // ++ 作为运算符
    var colors: Map[String, String] = colors1 ++ colors2
    println("colors1 ++ colors2 : " + colors) //colors1 ++ colors2 : Map(blue -> #0033FF, azure -> #F0FFFF, peru -> #CD853F, yellow -> #FFFF00, red -> #FF0000)

    // ++ 作为方法
    colors = colors1.++(colors2)
    println("colors1.++(colors2): " + colors) //colors1.++(colors2): Map(blue -> #0033FF, azure -> #F0FFFF, peru -> #CD853F, yellow -> #FFFF00, red -> #FF0000)
  }
}
