-- DROP TABLE IF EXISTS ads.ads_sku_favor_count_top3_by_tm;
CREATE EXTERNAL TABLE if not exists ads.ads_sku_favor_count_top3_by_tm
(
    `dt`          STRING COMMENT '统计日期',
    `tm_id`       STRING COMMENT '品牌ID',
    `tm_name`     STRING COMMENT '品牌名称',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `sku_name`    STRING COMMENT 'SKU名称',
    `favor_count` BIGINT COMMENT '被收藏次数',
    `rk`          BIGINT COMMENT '排名'
) COMMENT '各品牌商品收藏次数Top3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_sku_favor_count_top3_by_tm/';



insert overwrite table ads.ads_sku_favor_count_top3_by_tm
select * from ads.ads_sku_favor_count_top3_by_tm
union
select
    '${hiveconf:etl_date}' dt,
    tm_id,
    tm_name,
    sku_id,
    sku_name,
    favor_add_count_1d,
    rk
from
(
    select
        tm_id,
        tm_name,
        sku_id,
        sku_name,
        favor_add_count_1d,
        rank() over (partition by tm_id order by favor_add_count_1d desc) rk
    from dws.dws_interaction_sku_favor_add_1d
    where dt='${hiveconf:etl_date}'
)t1
where rk<=3;
