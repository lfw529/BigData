package com.lfw.kudu.supcon.sql

object Table_movie_info {
  val source: String =
    s"""
       |create table movie_info (
       |  `movie`    string,
       |  `category` string
       |)
       |TBLPROPERTIES (
       |    'streaming-source.enable' = 'true',
       |    'streaming-source.partition.include' = 'latest',
       |    'streaming-source.monitor-interval' = '12 h',
       |    'streaming-source.partition-order' = 'partition-name'
       |)
       |""".stripMargin

  val query: String =
    s"""
       |select * from movie_info
       |""".stripMargin
}
