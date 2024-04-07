DROP TABLE IF EXISTS ods_tmp.ods_base_region_full;
CREATE EXTERNAL TABLE ods_tmp.ods_base_region_full
(
    `id`               STRING COMMENT '地区ID',
    `region_name`      STRING COMMENT '地区名称',
    `create_time`      STRING COMMENT '创建时间',
    `operate_time`     STRING COMMENT '修改时间'
) COMMENT '地区表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_region_full/'
;


DROP TABLE IF EXISTS ods.ods_base_region_full;
CREATE EXTERNAL TABLE ods.ods_base_region_full
(
    `id`               STRING COMMENT '地区ID',
    `region_name`      STRING COMMENT '地区名称',
    `create_time`      STRING COMMENT '创建时间',
    `operate_time`     STRING COMMENT '修改时间'
) COMMENT '地区表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_region_full/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');