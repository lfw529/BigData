-- DROP TABLE IF EXISTS dws.dws_trade_user_payment_1d;
CREATE EXTERNAL TABLE if not exists dws.dws_trade_user_payment_1d
(
    `user_id`           STRING COMMENT '用户ID',
    `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
    `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
) COMMENT '交易域用户粒度支付最近1日汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_1d'
    TBLPROPERTIES ('orc.compress' = 'snappy');

--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dws层-交易域用户粒度支付最近1日汇总表
-- ********************************************************************

-- 首日装载
insert overwrite table dws.dws_trade_user_payment_1d partition(dt)
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount),
    dt
from dwd.dwd_trade_pay_detail_suc_inc
group by user_id,dt;


-- 每日装载
insert overwrite table dws.dws_trade_user_payment_1d partition(dt='${hiveconf:etl_date}')
select
    user_id,
    count(distinct(order_id)),
    sum(sku_num),
    sum(split_payment_amount)
from dwd.dwd_trade_pay_detail_suc_inc
where dt='${hiveconf:etl_date}'
group by user_id;