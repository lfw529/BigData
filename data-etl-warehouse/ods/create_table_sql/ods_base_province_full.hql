DROP TABLE IF EXISTS ods_tmp.ods_base_province_full;
CREATE EXTERNAL TABLE ods_tmp.ods_base_province_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '省份名称',
    `region_id`    STRING COMMENT '地区ID',
    `area_code`    STRING COMMENT '地区编码',
    `iso_code`     STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_3166_2`   STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '省份表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_province_full/';


DROP TABLE IF EXISTS ods.ods_base_province_full;
CREATE EXTERNAL TABLE ods.ods_base_province_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '省份名称',
    `region_id`    STRING COMMENT '地区ID',
    `area_code`    STRING COMMENT '地区编码',
    `iso_code`     STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_3166_2`   STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '省份表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_province_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');

