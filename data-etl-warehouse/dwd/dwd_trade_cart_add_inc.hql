-- DROP TABLE IF EXISTS dwd.dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE if not exists dwd.dwd_trade_cart_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '加购时间',
    `sku_num`     BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购事务事实表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC
LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
TBLPROPERTIES ('orc.compress' = 'snappy');



--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dwd层-交易域加购物车事务事实
-- ********************************************************************

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd.dwd_trade_cart_add_inc partition (dt)
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time,
    data.sku_num,
    date_format(data.create_time, 'yyyy-MM-dd')
from ods.ods_cart_info_inc
where dt = '2022-06-08' and type = 'bootstrap-insert';


-- 每日装载
insert overwrite table dwd.dwd_trade_cart_add_inc partition (dt = '${hiveconf:etl_date}')
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')                          date_id,
    date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')                 create_time,
    if(type = 'insert', data.sku_num, cast(data.sku_num as int) - cast(old['sku_num'] as int)) sku_num
from ods.ods_cart_info_inc
where dt = '${hiveconf:etl_date}'
  and (type = 'insert'
    or (type = 'update' and old['sku_num'] is not null and cast(data.sku_num as int) > cast(old['sku_num'] as int)));