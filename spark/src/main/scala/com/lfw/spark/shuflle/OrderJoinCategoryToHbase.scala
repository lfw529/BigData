package com.lfw.spark.shuflle

import com.alibaba.fastjson.{JSON, JSONException}
import com.lfw.spark.utils.HbaseUtils
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, client}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Connection
import java.util

/**
 * 1.JSON 的解析
 * 2.RDD 的 JOIN
 * 3.数据写入到 HBase
 */
object OrderJoinCategoryToHbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OrderJoinCategoryToHbase").setMaster("local[*]") // 本地模式，开启多线程
    //创建 SparkContext
    val sc = new SparkContext(conf)

    // 读取 order.json
    val orderLines = sc.textFile("spark/input/order.json")

    val orderBeanRDD: RDD[OrderBean] = orderLines.map(lines => {
      var orderBean: OrderBean = null;
      try {
        orderBean = JSON.parseObject(lines, classOf[OrderBean])
      } catch {
        case e: JSONException => {
          // 记录日志或写入其他存储系统
        }
      }
      orderBean
    })

    val filteredOrderBeanRDD = orderBeanRDD.filter(_ != null)

    // 读取分类数据
    val categoryLines = sc.textFile("spark/input/category.json")

    val categoryTpRDD = categoryLines.map(line => {
      var tp: (Int, String) = null
      try {
        val jsonObj = JSON.parseObject(line)
        val id = jsonObj.getInteger("id")
        val name = jsonObj.getString("name")
        tp = (id, name)
      } catch {
        case e: JSONException => {
          // 记录日志或写入其他存储系统
        }
      }
      tp
    })

    val filteredCategoryRDD = categoryTpRDD.filter(_ != null)

    val orderBeanTpRdd: RDD[(Int, OrderBean)] = filteredOrderBeanRDD.map(bean => {
      (bean.cid, bean)
    })

    // 使用左外连接
    val joined: RDD[(Int, (OrderBean, Option[String]))] = orderBeanTpRdd.leftOuterJoin(filteredCategoryRDD)

    // 写入到 Hbase [在 hbase 中建表: create 'order_detail', 'info', 'ext']
    joined.foreachPartition(it => {
      //创建 HbaseConnection
      val connection: client.Connection = HbaseUtils.getConnection;
      //创建 HTable
      val table: Table = connection.getTable(TableName.valueOf("order_detail"))
      val puts = new util.ArrayList[Put](10)
      it.foreach(e => {
        // 写入数据 (先将数据攒到结合中 Puts)
        val tp = e._2
        val bean = tp._1
        val name = tp._2.getOrElse("未知")
        // 创建put对象并指定row-key
        val put = new Put(Bytes.toBytes(bean.oid))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cid"), Bytes.toBytes(bean.cid))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cname"), Bytes.toBytes(name))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(bean.money))
        // 如果还有其他额外的信息要保存，可以保存到其他的数组
        // 将put对象添加到集合中
        puts.add(put)
        // 达到一定条件批量写入
        if (puts.size() == 10) {
          table.put(puts)  //批量写入
          puts.clear()
        }
      })
      if (puts.size() > 0) {
        table.put(puts)  //批量写入
      }
      // 关闭连接
      table.close()
      connection.close()
    })
  }
}

case class OrderBean(oid: String, cid: Int, money: Double)