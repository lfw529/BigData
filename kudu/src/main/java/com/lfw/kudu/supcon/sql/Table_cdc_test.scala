package com.lfw.kudu.supcon.sql

import com.lfw.kudu.supcon.config.Config

object Table_cdc_test {
  val source: String =
    s"""
       |create table cdc_test (
       |  id               int,
       |  val1             string,
       |  num_val          string,
       |  time_val         Timestamp(3)
       |)
       |WITH (
       |  'connector.type' = 'kudu',
       |  'kudu.masters' = '${Config.kuduBrokersTest}',
       |  'kudu.table' = 'rods.cdc_test',
       |  'kudu.primary-key-columns' = 'id',
       |  'kudu.hash-columns' = 'id'
       |)
       |""".stripMargin

  val query: String =
    s"""
       |select * from cdc_test
       |""".stripMargin
}
