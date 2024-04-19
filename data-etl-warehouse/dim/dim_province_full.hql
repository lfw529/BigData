-- DROP TABLE IF EXISTS dim.dim_province_full;
CREATE EXTERNAL TABLE if not exists dim.dim_province_full
(
    `id`            STRING COMMENT '省份ID',
    `province_name` STRING COMMENT '省份名称',
    `area_code`     STRING COMMENT '地区编码',
    `iso_code`      STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_3166_2`    STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `region_id`     STRING COMMENT '地区ID',
    `region_name`   STRING COMMENT '地区名称'
) COMMENT '地区维度表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dim/dim_province_full/'
TBLPROPERTIES ('orc.compress' = 'snappy');


--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dim层-地区维度-交易域
-- ********************************************************************
insert overwrite table dim.dim_province_full partition(dt='${hiveconf:etl_date}')
select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
(
    select
        id,
        name,
        region_id,
        area_code,
        iso_code,
        iso_3166_2
    from ods.ods_base_province_full
    where dt='${hiveconf:etl_date}'
)province
left join
(
    select
        id,
        region_name
    from ods.ods_base_region_full
    where dt='${hiveconf:etl_date}'
)region
on province.region_id=region.id;