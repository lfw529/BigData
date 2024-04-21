-- DROP TABLE IF EXISTS dwd.dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE if not exists dwd.dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
TBLPROPERTIES ("orc.compress" = "snappy");



--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dwd层-互动域收藏商品事务事实表
-- ********************************************************************

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
-- insert overwrite table dwd.dwd_interaction_favor_add_inc partition(dt)
-- select
--     data.id,
--     data.user_id,
--     data.sku_id,
--     date_format(data.create_time,'yyyy-MM-dd') date_id,
--     data.create_time,
--     date_format(data.create_time,'yyyy-MM-dd')
-- from ods.ods_favor_info_inc
-- where dt='2022-06-08'
--   and type = 'bootstrap-insert';


-- 每日装载
insert overwrite table dwd.dwd_interaction_favor_add_inc partition(dt='${hiveconf:etl_date}')
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time
from ods.ods_favor_info_inc
where dt='${hiveconf:etl_date}'
  and type = 'insert';