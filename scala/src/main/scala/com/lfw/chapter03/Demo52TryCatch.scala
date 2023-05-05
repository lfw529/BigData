package com.lfw.chapter03

import java.io.{FileNotFoundException, FileReader, IOException}

object Demo52TryCatch {
  def main(args: Array[String]): Unit = {
    try {
      val f = new FileReader("input.txt")
    } catch {
      case ex: FileNotFoundException => {
        println("Missing file exception") //Missing file exception
      }
      case ex: IOException => {
        println("IO Exception")
      }
    }
  }
}
