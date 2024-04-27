-- DROP TABLE IF EXISTS dws.dws_trade_user_order_td;
CREATE EXTERNAL TABLE if not exists dws.dws_trade_user_order_td
(
    `user_id`                   STRING COMMENT '用户ID',
    `order_date_first`          STRING COMMENT '历史至今首次下单日期',
    `order_date_last`           STRING COMMENT '历史至今末次下单日期',
    `order_count_td`            BIGINT COMMENT '历史至今下单次数',
    `order_num_td`              BIGINT COMMENT '历史至今购买商品件数',
    `original_amount_td`        DECIMAL(16, 2) COMMENT '历史至今下单原始金额',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '历史至今下单活动优惠金额',
    `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '历史至今下单优惠券优惠金额',
    `total_amount_td`           DECIMAL(16, 2) COMMENT '历史至今下单最终金额'
) COMMENT '交易域用户粒度订单历史至今汇总表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dws/dws_trade_user_order_td'
    TBLPROPERTIES ('orc.compress' = 'snappy');


--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dws层-交易域用户粒度订单历史至今汇总表
-- ********************************************************************

-- 首日装载
insert overwrite table dws.dws_trade_user_order_td partition(dt='2022-06-08')
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count,
    sum(order_num_1d) order_num,
    sum(order_original_amount_1d) original_amount,
    sum(activity_reduce_amount_1d) activity_reduce_amount,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount,
    sum(order_total_amount_1d) total_amount
from dws.dws_trade_user_order_1d
group by user_id;


--每日装载
-- 通过 full outer join 实现
insert overwrite table dws.dws_trade_user_order_td partition (dt = '${hiveconf:etl_date}')
select 
    nvl(old.user_id, new.user_id),
    if(old.user_id is not null, old.order_date_first, '${hiveconf:etl_date}'),
    if(new.user_id is not null, '${hiveconf:etl_date}', old.order_date_last),
    nvl(old.order_count_td, 0) + nvl(new.order_count_1d, 0),
    nvl(old.order_num_td, 0) + nvl(new.order_num_1d, 0),
    nvl(old.original_amount_td, 0) + nvl(new.order_original_amount_1d, 0),
    nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0),
    nvl(old.coupon_reduce_amount_td, 0) + nvl(new.coupon_reduce_amount_1d, 0),
    nvl(old.total_amount_td, 0) + nvl(new.order_total_amount_1d, 0)
from 
(
     select 
        user_id,
        order_date_first,
        order_date_last,
        order_count_td,
        order_num_td,
        original_amount_td,
        activity_reduce_amount_td,
        coupon_reduce_amount_td,
        total_amount_td
     from dws.dws_trade_user_order_td
     where dt = date_add('${hiveconf:etl_date}', -1)
) old
full outer join
(
    select 
        user_id,
        order_count_1d,
        order_num_1d,
        order_original_amount_1d,
        activity_reduce_amount_1d,
        coupon_reduce_amount_1d,
        order_total_amount_1d
    from dws.dws_trade_user_order_1d
    where dt = '${hiveconf:etl_date}'
) new on old.user_id = new.user_id;