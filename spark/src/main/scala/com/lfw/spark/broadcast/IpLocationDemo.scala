package com.lfw.spark.broadcast

import com.lfw.spark.utils.{IpUtils, SparkUtil}
import java.io.File
import org.apache.spark.broadcast.Broadcast

import scala.io.Source

/**
 * 根据 IP 的地址规则，查找指定 IP 地址的归属地
 */
object IpLocationDemo {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getContext("IpLocationDemo", isLocal = true)

    /**
     * 1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
     * 1.0.8.0|1.0.15.255|16779264|16781311|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178
     * 1.0.32.0|1.0.63.255|16785408|16793599|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178
     * 1.1.0.0|1.1.0.255|16842752|16843007|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
     */
    //读取IP规则数据
    val ipLines = sc.textFile("spark/input/broadcast/ip.txt")

    //使用spark触发一次Action来读取要广播的数据
    //    val ipRulesInDriver: Array[(Long, Long, String, String)] = ipLines.map(e => {
    //      val fields = e.split("[|]")
    //      val startNum = fields(2).toLong
    //      val endNum = fields(3).toLong
    //      val province = fields(6)
    //      val city = fields(7)
    //      (startNum, endNum, province, city)
    //    }).collect()

    //使用scala io读取数据
    val source = Source.fromFile(new File("spark/input/broadcast/ip.txt"))
    val it = source.getLines()
    val ipRulesInDriver: Array[(Long, Long, String, String)] = it.map(e => {
      val fields = e.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      val city = fields(7)
      (startNum, endNum, province, city)
    }).toArray
    source.close()

    //将Driver端的数据广播到Executor
    //要广播的数据被发送到了Executor中，然后将广播到Executor的数据的地址引用信息返回Driver端
    //该方法是同步阻塞的，如果没有广播完，方法不会继续向下执行
    val broadcastRef: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(ipRulesInDriver)

    //读取要处理的日志数据
    val ipLogRDD = sc.textFile("spark/input/broadcast/ipaccess.log")
    val ipAndOne = ipLogRDD.map(e => {
      val fields = e.split("[|]")
      val ip = fields(1)
      val ipNum = IpUtils.ip2Long(ip)
      //根据返回到Driver端的广播变量的描述信息（数据的地址信息），可以获取到实现广播数据引用
      val ipRulesInExecutor: Array[(Long, Long, String, String)] = broadcastRef.value
      //使用二分法进行查找
      var province = "未知"
      val index = IpUtils.binarySearch(ipRulesInExecutor, ipNum)
      if (index >= 0) {
        province = ipRulesInExecutor(index)._3
      }
      (province, 1)
    })

    val reduced = ipAndOne.reduceByKey(_ + _)

    val res = reduced.collect()

    println(res.toBuffer)

    //释放广播变量
    broadcastRef.unpersist()

    //获取还有其他的运算逻辑 ...
    sc.stop()
  }
}
