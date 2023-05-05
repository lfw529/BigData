package com.lfw.chapter03

import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException

object Demo53Finally {
  def main(args: Array[String]) {
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException => {
        println("Missing file exception") //Missing file exception
      }
      case ex: IOException => {
        println("IO Exception")
      }
    } finally {
      println("Exiting finally...") //Exiting finally...
    }
  }
}
