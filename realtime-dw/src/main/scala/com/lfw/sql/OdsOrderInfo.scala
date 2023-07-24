package com.lfw.sql

import com.lfw.config.MysqlConfig

object OdsOrderInfo {

  val source: String =
    s"""
       |create table ods_order_detail
       |(
       |  id bigint,                        --'编号'
       |  order_id bigint,                  --'订单编号'
       |  sku_id bigint,                    --'sku_id'
       |  sku_name varchar(200),            --'sku名称(冗余)'
       |  img_url  varchar(200),            --'图片名称(冗余)'
       |  order_price  decimal(10,2),       --'购买价格(下单时sku价格)'
       |  sku_num  bigint,                  --'购买个数'
       |  create_time  TIMESTAMP(3),        --'创建时间'
       |  source_type  varchar(20),         --'来源类型'
       |  source_id  bigint,                --'来源编号'
       |  split_total_amount decimal(16,2),
       |  split_activity_amount decimal(16,2),
       |  split_coupon_amount decimal(16,2),
       |  primary key(id) not enforced
       |)
       |WITH (
       |  'connector' = 'mysql-cdc',
       |  'hostname' = '${MysqlConfig.Hostname}',
       |  'port' = '${MysqlConfig.Port}',
       |  'username' = '${MysqlConfig.username}',
       |  'password' = '${MysqlConfig.password}',
       |  'database-name' = 'gmall',
       |  'table-name' = 'order_detail'
       |)
       |""".stripMargin

  val sink: String =
    s"""
       |create table ods_order_info_sinkTokafka
       |(
       |  id bigint,                        --'编号'
       |  order_id bigint,                  --'订单编号'
       |  sku_id bigint,                    --'sku_id'
       |  sku_name varchar(200),            --'sku名称(冗余)'
       |  img_url  varchar(200),            --'图片名称(冗余)'
       |  order_price  decimal(10,2),       --'购买价格(下单时sku价格)'
       |  sku_num  bigint,                  --'购买个数'
       |  create_time  TIMESTAMP(3),        --'创建时间'
       |  source_type  varchar(20),         --'来源类型'
       |  source_id  bigint,                --'来源编号'
       |  split_total_amount decimal(16,2),
       |  split_activity_amount decimal(16,2),
       |  split_coupon_amount decimal(16,2)
       |)
       |WITH (
       |  'connector' = 'kafka',
       |  'topic' = 'ods_order_info',
       |  'properties.bootstrap.servers' = 'hadoop102:9092',
       |  'properties.group.id' = 'lfw',
       |  'scan.startup.mode' = 'earliest-offset',
       |  'value.format' = 'debezium-json'
       |)
       |""".stripMargin

  val transform: String =
    s"""
      | insert into ods_order_info_sinkTokafka select * from ods_order_info
    """.stripMargin
}
