package com.lfw.chapter03

object Demo56ImplicitClass {
  def main(args: Array[String]): Unit = {
    import com.lfw.chapter03.StringUtils.StringImprovement

    println("lfw".increment)
  }
}

object StringUtils {

  implicit class StringImprovement(val s: String) { //隐式类
    def increment: String = s.map(x => (x + 1).toChar)
  }

}
