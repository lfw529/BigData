-- DROP TABLE IF EXISTS dwd.dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE if not exists dwd.dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT '商品ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`      STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `date_id`               STRING COMMENT '下单日期ID',
    `create_time`           STRING COMMENT '下单时间',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单事务事实表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
TBLPROPERTIES ('orc.compress' = 'snappy');


--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dwd层-交易域下单事务事实表
-- ********************************************************************

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
-- insert overwrite table dwd.dwd_trade_order_detail_inc partition (dt)
-- select
--     od.id,
--     order_id,
--     user_id,
--     sku_id,
--     province_id,
--     activity_id,
--     activity_rule_id,
--     coupon_id,
--     date_format(create_time, 'yyyy-MM-dd') date_id,
--     create_time,
--     sku_num,
--     split_original_amount,
--     nvl(split_activity_amount,0.0),
--     nvl(split_coupon_amount,0.0),
--     split_total_amount,
--     date_format(create_time,'yyyy-MM-dd')
-- from
-- (
--     select
--         data.id,
--         data.order_id,
--         data.sku_id,
--         data.create_time,
--         data.sku_num,
--         data.sku_num * data.order_price split_original_amount,
--         data.split_total_amount,
--         data.split_activity_amount,
--         data.split_coupon_amount
--     from ods.ods_order_detail_inc
--     where dt = '2022-06-08' and type = 'bootstrap-insert'
-- ) od
-- left join
-- (
--     select
--         data.id,
--         data.user_id,
--         data.province_id
--     from ods.ods_order_info_inc
--     where dt = '2022-06-08' and type = 'bootstrap-insert'
-- ) oi on od.order_id = oi.id
-- left join
-- (
--     select
--         data.order_detail_id,
--         data.activity_id,
--         data.activity_rule_id
--     from ods.ods_order_detail_activity_inc
--     where dt = '2022-06-08' and type = 'bootstrap-insert'
-- ) act on od.id = act.order_detail_id
-- left join
-- (
--     select
--         data.order_detail_id,
--         data.coupon_id
--     from ods.ods_order_detail_coupon_inc
--     where dt = '2022-06-08' and type = 'bootstrap-insert'
-- ) cou on od.id = cou.order_detail_id;


-- 每日装载
insert overwrite table dwd.dwd_trade_order_detail_inc partition (dt='${hiveconf:etl_date}')
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_id,
    create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
(
    select
        data.id,
        data.order_id,
        data.sku_id,
        date_format(data.create_time, 'yyyy-MM-dd') date_id,
        data.create_time,
        data.sku_num,
        data.sku_num * data.order_price split_original_amount,
        data.split_total_amount,
        data.split_activity_amount,
        data.split_coupon_amount
    from ods.ods_order_detail_inc
    where dt = '${hiveconf:etl_date}'
      and type = 'insert'
) od
left join
(
    select
        data.id,
        data.user_id,
        data.province_id
    from ods.ods_order_info_inc
    where dt = '${hiveconf:etl_date}' and type = 'insert'
) oi on od.order_id = oi.id
left join
(
    select
        data.order_detail_id,
        data.activity_id,
        data.activity_rule_id
    from ods.ods_order_detail_activity_inc
    where dt = '${hiveconf:etl_date}' and type = 'insert'
) act
on od.id = act.order_detail_id
left join
(
    select
        data.order_detail_id,
        data.coupon_id
    from ods.ods_order_detail_coupon_inc
    where dt = '${hiveconf:etl_date}' and type = 'insert'
) cou on od.id = cou.order_detail_id;