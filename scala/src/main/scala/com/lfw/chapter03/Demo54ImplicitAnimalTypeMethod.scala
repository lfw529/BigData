package com.lfw.chapter03

class SwingType {
  def wantLearned(sw: String): Unit = println("兔子已经学会了:" + sw)
}

object swimming {
  implicit def learningType(s: Demo54ImplicitAnimalTypeMethod): SwingType = new SwingType
}

class Demo54ImplicitAnimalTypeMethod

object Demo54ImplicitAnimalTypeMethod {
  def main(args: Array[String]): Unit = {
    import com.lfw.chapter03.swimming.learningType

    val rabbit = new Demo54ImplicitAnimalTypeMethod
    rabbit.wantLearned("breaststroke") //蛙泳
  }
}
