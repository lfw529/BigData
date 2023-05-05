package com.lfw.chapter03

object Demo36SimpleFunction {
  def main(args: Array[String]): Unit = {
    val list = List(5, 1, 8, 2, -3, 4)
    val list2 = List(("a", 5), ("b", 1), ("c", 8), ("d", 2), ("e", 3), ("f", 4))

    //1.求和
    var sum = 0
    for (elem <- list) {
      sum += elem
    }
    println(sum) //17
    //直接用封装
    println(list.sum) //17
    println("-------------------")
    //2.求乘积
    println(list.product) //-960
    println("-------------------")
    //3.最大值
    println(list.max) //8
    //带key求value最大用maxBy
    println(list2.maxBy((tuple: (String, Int)) => tuple._2)) //(c, 8)
    //省略版
    println(list2.maxBy(_._2)) //(c, 8)
    println("-------------------")
    //4.最小值
    println(list.min) // -3
    //带key求value最小用minBy
    println(list2.minBy(_._2)) //(b, 1)
    println("-------------------")
    //5.排序
    //5.1 sorted
    val sortedList: List[Int] = list.sorted
    println(sortedList) //List(-3, 1, 2, 4, 5, 8)
    //从大到小逆序排序 (两个阶段，效率低)
    println(list.sorted.reverse) //List(8, 5, 4, 2, 1, -3)
    //传入隐式参数 (一个阶段，效率高)
    println(list.sorted(Ordering[Int].reverse)) //List(8, 5, 4, 2, 1, -3)
    //元组排序是按照key排序
    println(list2.sorted) //List((a,5), (b,1), (c,8), (d,2), (e,3), (f,4))
    println("-------------------")
    //5.2 sortBy
    //元组排序是按照value排序
    println(list2.sortBy(_._2)) //List((b,1), (d,2), (e,3), (f,4), (a,5), (c,8))
    //元组排序是按照value排序，倒序
    println(list2.sortBy(_._2)(Ordering[Int].reverse)) //List((c,8), (a,5), (f,4), (e,3), (d,2), (b,1))
    println("-------------------")
    //5.3 sortWith
    println(list.sortWith((a: Int, b: Int) => {
      a < b
    }))   //List(-3, 1, 2, 4, 5, 8)
    println("-------------------")
    //正序排序
    println(list.sortWith(_ < _)) //List(-3, 1, 2, 4, 5, 8)
    //倒序排序
    println(list.sortWith(_ > _)) //List(8, 5, 4, 2, 1, -3)
  }
}
