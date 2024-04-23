-- DROP TABLE IF EXISTS dws.dws_trade_user_cart_add_1d;
CREATE EXTERNAL TABLE dws.dws_trade_user_cart_add_1d
(
    `user_id`           STRING COMMENT '用户id',
    `cart_add_count_1d` BIGINT COMMENT '最近1日加购次数',
    `cart_add_num_1d`   BIGINT COMMENT '最近1日加购商品件数'
) COMMENT '交易域用户粒度加购最近1日汇总事实表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dws/dws_trade_user_cart_add_1d'
TBLPROPERTIES ('orc.compress' = 'snappy');


--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dws层-交易域用户粒度加购最近1日汇总事实表
-- ********************************************************************

-- 首日装载
insert overwrite table dws.dws_trade_user_cart_add_1d partition(dt)
select
    user_id,
    count(*),
    sum(sku_num),
    dt
from dwd.dwd_trade_cart_add_inc
group by user_id,dt;



-- 每日装载
insert overwrite table dws.dws_trade_user_cart_add_1d partition(dt='${hiveconf:etl_date}')
select
    user_id,
    count(*),
    sum(sku_num)
from dwd.dwd_trade_cart_add_inc
where dt='${hiveconf:etl_date}'
group by user_id;