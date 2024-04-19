-- DROP TABLE IF EXISTS dim.dim_promotion_pos_full;
CREATE EXTERNAL TABLE if not exists dim.dim_promotion_pos_full
(
    `id`             STRING COMMENT '营销坑位ID',
    `pos_location`   STRING COMMENT '营销坑位位置',
    `pos_type`       STRING COMMENT '营销坑位类型 ',
    `promotion_type` STRING COMMENT '营销类型',
    `create_time`    STRING COMMENT '创建时间',
    `operate_time`   STRING COMMENT '修改时间'
) COMMENT '营销坑位维度表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dim/dim_promotion_pos_full/'
TBLPROPERTIES ('orc.compress' = 'snappy');



--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dim层-营销坑位维度-交易域
-- ********************************************************************
insert overwrite table dim.dim_promotion_pos_full partition(dt='${hiveconf:etl_date}')
select
    `id`,
    `pos_location`,
    `pos_type`,
    `promotion_type`,
    `create_time`,
    `operate_time`
from ods.ods_promotion_pos_full
where dt='${hiveconf:etl_date}';