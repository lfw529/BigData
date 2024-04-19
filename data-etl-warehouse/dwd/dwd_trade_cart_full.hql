-- DROP TABLE IF EXISTS dwd.dwd_trade_cart_full;
CREATE EXTERNAL TABLE if not exists dwd.dwd_trade_cart_full
(
    `id`         STRING COMMENT '编号',
    `user_id`   STRING COMMENT '用户ID',
    `sku_id`    STRING COMMENT 'SKU_ID',
    `sku_name`  STRING COMMENT '商品名称',
    `sku_num`   BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
TBLPROPERTIES ('orc.compress' = 'snappy');


--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dwd层-交易域购物车周期快照事实
-- ********************************************************************

insert overwrite table dwd.dwd_trade_cart_full partition(dt='${hiveconf:etl_date}')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods.ods_cart_info_full
where dt='${hiveconf:etl_date}'
  and is_ordered='0';