package com.lfw.chapter03

object Demo10ListReverse {
  def main(args: Array[String]): Unit = {
    val site: List[String] = "lfw" :: ("Google" :: ("Baidu" :: Nil))
    println("site 反转前：" + site)

    println("site 反转后：" + site.reverse)
  }
}
