package com.lfw.chapter03

object Demo22MapKey {
  def main(args: Array[String]): Unit = {
    val sites = Map(
      "lfw" -> "http://www.lfw.com",
      "baidu" -> "http://www.baidu.com",
      "taobao" -> "http://www.taobao.com")
    if (sites.contains("lfw")) {
      println("lfw 键存在，对应的值为: " + sites("lfw"))
    } else {
      println("lfw 键不存在")
    }
    if (sites.contains("baidu")) {
      println("baidu 键存在，对应的值为: " + sites("baidu"))
    } else {
      println("baidu 键不存在")
    }
    if (sites.contains("google")) {
      println("google 键存在，对应的值为: " + sites("google"))
    } else {
      println("google 键不存在")
    }
    //lfw 键存在，对应的值为: http://www.lfw.com
    //baidu 键存在，对应的值为: http://www.baidu.com
    //google 键不存在
  }
}
