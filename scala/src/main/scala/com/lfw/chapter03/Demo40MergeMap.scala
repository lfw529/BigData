package com.lfw.chapter03

import scala.collection.mutable

object Demo40MergeMap {
  def main(args: Array[String]): Unit = {
    val map1 = Map("a" -> 1, "b" -> 3, "c" -> 6)
    val map2: mutable.Map[String, Int] = mutable.Map("a" -> 6, "b" -> 2, "c" -> 9, "d" -> 3)
    println(map1 ++ map2) //这种追加是覆盖，不会将值累计 //Map(a -> 6, b -> 2, c -> 9, d -> 3)
    println("-----------------------------------------")
    val map3: mutable.Map[String, Int] = map1.foldLeft(map2)(
      (mergedMap, kv) => {
        val key: String = kv._1
        val value: Int = kv._2
        mergedMap(key) = mergedMap.getOrElse(key, 0) + value
        mergedMap
      }
    )
    println(map3) //Map(b -> 5, d -> 3, a -> 7, c -> 15)
  }
}
