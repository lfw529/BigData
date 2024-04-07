DROP TABLE IF EXISTS ods_tmp.ods_base_dic_full;
CREATE EXTERNAL TABLE ods_tmp.ods_base_dic_full
(
    `dic_code`     STRING COMMENT '编号',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编号',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '修改日期'
) COMMENT '编码字典表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_dic_full/';



DROP TABLE IF EXISTS ods.ods_base_dic_full;
CREATE EXTERNAL TABLE ods.ods_base_dic_full
(
    `dic_code`     STRING COMMENT '编号',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编号',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '修改日期'
) COMMENT '编码字典表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
NULL DEFINED AS ''
LOCATION '/warehouse/gmall/ods/ods_base_dic_full/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');