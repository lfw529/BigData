package com.lfw.chapter03

class SwingType1 {
  def wantLearned(sw: String): Unit = println("兔子已经学会了:" + sw)
}

class Demo55ImplicitAnimalTypePackage

object Demo55ImplicitAnimalTypePackage {
  def main(args: Array[String]): Unit = {
    import com.lfw.chapter03.swimmingPackage.swimming.learningType

    val rabbit = new Demo55ImplicitAnimalTypePackage
    rabbit.wantLearned("breaststroke") //蛙泳
  }
}

object swimming {
    implicit def learningType(s: Demo55ImplicitAnimalTypePackage): SwingType1 = new SwingType1 //将转换函数定义在包中
  }
