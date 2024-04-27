-- DROP TABLE IF EXISTS dws.dws_trade_province_order_nd;
CREATE EXTERNAL TABLE if not exists dws.dws_trade_province_order_nd
(
    `province_id`                STRING COMMENT '省份ID',
    `province_name`              STRING COMMENT '省份名称',
    `area_code`                  STRING COMMENT '地区编码',
    `iso_code`                   STRING COMMENT '旧版国际标准地区编码',
    `iso_3166_2`                 STRING COMMENT '新版国际标准地区编码',
    `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
    `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
    `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
    `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
    `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
    `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
    `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
    `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
    `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
) COMMENT '交易域省份粒度订单最近n日汇总表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dws/dws_trade_province_order_nd'
TBLPROPERTIES ('orc.compress' = 'snappy');


--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dws层-交易域省份粒度订单最近n日汇总表
-- ********************************************************************
insert overwrite table dws.dws_trade_province_order_nd partition(dt='${hiveconf:etl_date}')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt>=date_add('${hiveconf:etl_date}',-6),order_count_1d,0)),
    sum(if(dt>=date_add('${hiveconf:etl_date}',-6),order_original_amount_1d,0)),
    sum(if(dt>=date_add('${hiveconf:etl_date}',-6),activity_reduce_amount_1d,0)),
    sum(if(dt>=date_add('${hiveconf:etl_date}',-6),coupon_reduce_amount_1d,0)),
    sum(if(dt>=date_add('${hiveconf:etl_date}',-6),order_total_amount_1d,0)),
    sum(order_count_1d),
    sum(order_original_amount_1d),
    sum(activity_reduce_amount_1d),
    sum(coupon_reduce_amount_1d),
    sum(order_total_amount_1d)
from dws.dws_trade_province_order_1d
where dt>=date_add('${hiveconf:etl_date}',-29)
  and dt<='${hiveconf:etl_date}'
group by province_id,province_name,area_code,iso_code,iso_3166_2;