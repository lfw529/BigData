package com.lfw.chapter03

import scala.collection.immutable

object Demo06ListOperation {
  def main(args: Array[String]): Unit = {
    val site: List[String] = "lfw" :: ("Google" :: ("Baidu" :: Nil))
    val nums: immutable.Seq[Nothing] = Nil

    println("第一网站是 : " + site.head)  //第一网站是 : lfw
    println("最后一个网站是 : " + site.tail)  //最后一个网站是 : List(Google, Baidu)
    println("查看列表 site 是否为空 : " + site.isEmpty)  //查看列表 site 是否为空 : false
    println("查看 nums 是否为空 : " + nums.isEmpty)  //查看 nums 是否为空 : true
  }
}
