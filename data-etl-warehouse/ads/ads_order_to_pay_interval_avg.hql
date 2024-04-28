-- DROP TABLE IF EXISTS ads.ads_order_to_pay_interval_avg;
CREATE EXTERNAL TABLE if not exists ads.ads_order_to_pay_interval_avg
(
    `dt`                        STRING COMMENT '统计日期',
    `order_to_pay_interval_avg` BIGINT COMMENT '下单到支付时间间隔平均值,单位为秒'
) COMMENT '下单到支付时间间隔平均值统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_order_to_pay_interval_avg/';



insert overwrite table ads.ads_order_to_pay_interval_avg
select * from ads.ads_order_to_pay_interval_avg
union
select
    '${hiveconf:etl_date}',
    cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
from dwd.dwd_trade_trade_flow_acc
where dt in ('9999-12-31','${hiveconf:etl_date}')
and payment_date_id='${hiveconf:etl_date}';