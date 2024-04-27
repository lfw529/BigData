-- DROP TABLE IF EXISTS ads.ads_new_order_user_stats;
CREATE EXTERNAL TABLE if not exists ads.ads_new_order_user_stats
(
    `dt`                   STRING COMMENT '统计日期',
    `recent_days`          BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count` BIGINT COMMENT '新增下单人数'
) COMMENT '新增下单用户统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_new_order_user_stats/';


insert overwrite table ads.ads_new_order_user_stats
select * from ads.ads_new_order_user_stats
union
select
    '${hiveconf:etl_date}' dt,
    recent_days,
    count(*) new_order_user_count
from dws.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
where dt='${hiveconf:etl_date}'
  and order_date_first>=date_add('${hiveconf:etl_date}',-recent_days+1)
group by recent_days;



