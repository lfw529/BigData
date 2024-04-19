-- DROP TABLE IF EXISTS dim.dim_promotion_refer_full;
CREATE EXTERNAL TABLE if not exists dim.dim_promotion_refer_full
(
    `id`           STRING COMMENT '营销渠道ID',
    `refer_name`   STRING COMMENT '营销渠道名称',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '营销渠道维度表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dim/dim_promotion_refer_full/'
TBLPROPERTIES ('orc.compress' = 'snappy');

--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dim层-营销坑位维度-交易域
-- ********************************************************************
insert overwrite table dim.dim_promotion_refer_full partition(dt='${hiveconf:etl_date}')
select
    `id`,
    `refer_name`,
    `create_time`,
    `operate_time`
from ods.ods_promotion_refer_full
where dt='${hiveconf:etl_date}';