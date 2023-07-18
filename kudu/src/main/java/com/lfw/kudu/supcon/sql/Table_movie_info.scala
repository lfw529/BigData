package com.lfw.kudu.supcon.sql

object Table_movie_info {
  val source: String =
    s"""
       |create table movie_info (
       |  `movie`    string,
       |  `category` string
       |)
       |WITH (
       |    'connector' = 'jdbc',
       |    'lookup.cache.ttl' = '10000',
       |    'tablename' = 'movie-info',
       |    'url' = 'jdbc:hive2://hadoop102:10000/default'
       |)
       |""".stripMargin

  val query: String =
    s"""
       |select * from movie_info
       |""".stripMargin
}
