DROP TABLE IF EXISTS ods_tmp.ods_coupon_use_inc;
CREATE EXTERNAL TABLE ods_tmp.ods_coupon_use_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,
                  coupon_id :STRING,
                  user_id :STRING,
                  order_id :STRING,
                  coupon_status :STRING,
                  get_time :STRING,
                  using_time:STRING,
                  used_time :STRING,expire_time :STRING,
                  create_time :STRING,
                  operate_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '优惠券领用表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_coupon_use_inc/'
;


DROP TABLE IF EXISTS ods.ods_coupon_use_inc;
CREATE EXTERNAL TABLE ods.ods_coupon_use_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,
                  coupon_id :STRING,
                  user_id :STRING,
                  order_id :STRING,
                  coupon_status :STRING,
                  get_time :STRING,
                  using_time:STRING,
                  used_time :STRING,expire_time :STRING,
                  create_time :STRING,
                  operate_time :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '优惠券领用表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_coupon_use_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');