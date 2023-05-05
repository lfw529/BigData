package com.lfw.chapter03

object Demo29Option {
  def main(args: Array[String]): Unit = {
    val sites = Map("lfw" -> "www.lfw.com", "google" -> "www.google.com")

    println("sites.get( \"lfw\" ) : " + sites.get("lfw")) // Some(www.lfw.com)
    println("sites.get( \"baidu\" ) : " + sites.get("baidu")) //  None
  }
}
