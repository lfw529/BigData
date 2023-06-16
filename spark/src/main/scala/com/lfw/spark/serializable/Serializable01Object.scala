package com.lfw.spark.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Serializable01Object {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("Object").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //在driver中创建
    val u1 = new User
    u1.name = "zhangsan"
    val u2 = new User
    u2.name = "lisi"
    val u3 = new User
    u3.name = "lifuwen"

    val rdd: RDD[User] = sc.makeRDD(List(u1, u2))
    val rdd2: RDD[User] = sc.makeRDD(List())

    //因为在executor端用到了driver端的user对象,因此这些user对象要序列化
    //序列化方式
    //1 手动继承 extends Serializable
    //2 使用样例类  样例类自动实现了序列化
    rdd.foreach(user => println(user.name)) //由于多线程问题，所以打印结果是无序的
    println("----------------")
    //因为rdd2里面没有数据,所以foreach算子里面的代码没有执行,那就说明我在executor端没有用到了driver端的user对象,所以不报错
    rdd2.foreach(user => println(user.name))
    println("----------------")
    //3.3 闭包检查问题
    //这个时候foreach算子里面的代码也没有执行  但是报错了，打印，ERROR Task not serializable
    //注意：此段代码没执行就报错了,因为spark自带闭包检查
    rdd.foreach(user => println(user.name + " love " + u3.name))

    //TODO 3 关闭资源
    sc.stop()
  }
}

/**
 * 如果你自定义了一个类,然后想把这个类的对象放到rdd里面运行,请你务必序列化这个类
 * 或者 直接用样例类
 *
 */
case class User()  { //java 底层序列化接口
  var name: String = _
}
