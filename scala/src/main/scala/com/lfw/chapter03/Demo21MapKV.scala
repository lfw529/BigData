package com.lfw.chapter03

object Demo21MapKV {
  def main(args: Array[String]): Unit = {
    val sites = Map("lfw" -> "http://www.lfw.com",
      "baidu" -> "http://www.baidu.com",
      "taobao" -> "http://www.taobao.com")

    sites.keys.foreach { i =>
      print("Key = " + i)
      println(" Value = " + sites(i))
    }

    //Key = lfw Value = http://www.lfw.com
    //Key = baidu Value = http://www.baidu.com
    //Key = taobao Value = http://www.taobao.com
  }
}
