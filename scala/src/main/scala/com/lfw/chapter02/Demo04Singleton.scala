package com.lfw.chapter02

object Demo04Singleton {
  def main(args: Array[String]): Unit = {
    val s1: Singleton = Singleton.getInstance()
    val s2: Singleton = Singleton.getInstance()
    s1.x = 6
    println(s1 == s2)
    println(s1.x)
    println(s2.x)
  }
}

class Singleton /*private()*/ {
  var x = 5
}

object Singleton {
  private val s = new Singleton

  def getInstance(): Singleton = {
    s
  }
}

