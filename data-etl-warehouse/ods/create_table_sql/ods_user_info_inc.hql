DROP TABLE IF EXISTS ods_tmp.ods_user_info_inc;
CREATE EXTERNAL TABLE ods_tmp.ods_user_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,
                  login_name :STRING,
                  nick_name :STRING,
                  passwd :STRING,
                  name :STRING,
                  phone_num :STRING,
                  email:STRING,
                  head_img :STRING,
                  user_level :STRING,
                  birthday :STRING,
                  gender :STRING,
                  create_time :STRING,
                  operate_time:STRING,
                  status :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '用户表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_user_info_inc/'
;


DROP TABLE IF EXISTS ods.ods_user_info_inc;
CREATE EXTERNAL TABLE ods.ods_user_info_inc
(
    `type` STRING COMMENT '变动类型',
    `ts`   BIGINT COMMENT '变动时间',
    `data` STRUCT<id :STRING,
                  login_name :STRING,
                  nick_name :STRING,
                  passwd :STRING,
                  name :STRING,
                  phone_num :STRING,
                  email:STRING,
                  head_img :STRING,
                  user_level :STRING,
                  birthday :STRING,
                  gender :STRING,
                  create_time :STRING,
                  operate_time:STRING,
                  status :STRING> COMMENT '数据',
    `old`  MAP<STRING,STRING> COMMENT '旧值'
) COMMENT '用户表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
LOCATION '/warehouse/gmall/ods/ods_user_info_inc/'
    TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');