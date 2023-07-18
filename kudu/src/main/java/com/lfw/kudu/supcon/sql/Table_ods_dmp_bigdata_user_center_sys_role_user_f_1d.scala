package com.lfw.kudu.supcon.sql

import com.lfw.kudu.supcon.config.Config

object Table_ods_dmp_bigdata_user_center_sys_role_user_f_1d {
  val source: String =
    s"""
       |set 'table.sql-dialect'='hive';
       |create table ods_dmp_bigdata_user_center_sys_role_user_f_1d (
       |  id            bigint comment 'id',
       |  role_id       bigint comment '角色id',
       |  user_id       bigint comment '用户id'
       |)
       |PARTITIONED BY (pt_day string)
       |TBLPROPERTIES (
       |    'streaming-source.enable' = 'true',
       |    'streaming-source.partition.include' = 'latest',
       |    'streaming-source.monitor-interval' = '12 h',
       |    'streaming-source.partition-order' = 'partition-name', -- 默认选项，可以忽略
       |);
       |""".stripMargin

  val query: String =
    s"""
       |select * from ods_dmp_bigdata_user_center_sys_role_user_f_1d
       |""".stripMargin
}
