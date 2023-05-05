package com.lfw.chapter03

object Demo14SetConcat {
  def main(args: Array[String]): Unit = {
    val site1 = Set("lfw", "Google", "Baidu")
    val site2 = Set("Faceboook", "Taobao")

    // ++ 作为运算符使用
    var site: Set[String] = site1 ++ site2
    println("site1 ++ site2: " + site) //site1 ++ site2: Set(Faceboook, lfw, Taobao, Google, Baidu)

    //  ++ 作为方法使用
    site = site1.++(site2)
    println("site1.++(site2): " + site) //site1.++(site2): Set(Faceboook, lfw, Taobao, Google, Baidu)
  }
}
