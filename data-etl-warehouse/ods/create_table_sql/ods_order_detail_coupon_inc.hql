DROP TABLE IF EXISTS ods_tmp.ods_order_detail_coupon_inc;
CREATE EXTERNAL TABLE ods_tmp.ods_order_detail_coupon_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,
                  order_id :STRING,
                  order_detail_id :STRING,
                  coupon_id :STRING,
                  coupon_use_id :STRING,
                  sku_id:STRING,
                  create_time :STRING,
                  operate_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细优惠券关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon_inc/'
;


DROP TABLE IF EXISTS ods.ods_order_detail_coupon_inc;
CREATE EXTERNAL TABLE ods.ods_order_detail_coupon_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,
                  order_id :STRING,
                  order_detail_id :STRING,
                  coupon_id :STRING,
                  coupon_use_id :STRING,
                  sku_id:STRING,
                  create_time :STRING,
                  operate_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '订单明细优惠券关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');