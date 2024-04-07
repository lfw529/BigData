DROP TABLE IF EXISTS ods_tmp.ods_base_category3_full;
CREATE EXTERNAL TABLE ods_tmp.ods_base_category3_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '三级分类名称',
    `category2_id` STRING COMMENT '二级分类编号',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '三级品类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_category3_full/'
;


DROP TABLE IF EXISTS ods.ods_base_category3_full;
CREATE EXTERNAL TABLE ods.ods_base_category3_full
(
    `id`           STRING COMMENT '编号',
    `name`         STRING COMMENT '三级分类名称',
    `category2_id` STRING COMMENT '二级分类编号',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '三级品类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_category3_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');