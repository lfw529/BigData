package com.lfw.chapter03

object Demo13SetOperation {
  def main(args: Array[String]): Unit = {
    val site = Set("lfw", "Google", "Baidu")
    val nums: Set[Int] = Set()

    println("第一网站是: " + site.head) //第一网站是: lfw
    println("最后一个网站是: " + site.tail) //最后一个网站是: Set(Google, Baidu)
    println("查看列表 site 是否为空: " + site.isEmpty) //查看列表 site 是否为空: false
    println("查看 nums 是否为空: " + nums.isEmpty) //查看 nums 是否为空: true
  }
}
