DROP TABLE IF EXISTS ads.ads_user_change;
CREATE EXTERNAL TABLE if not exists ads.ads_user_change
(
    dt               STRING COMMENT '统计日期',
    user_churn_count BIGINT COMMENT '流失用户数',
    user_back_count  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_user_change/';




insert overwrite table ads.ads_user_change
select * from ads.ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '${hiveconf:etl_date}' dt,
        count(*) user_churn_count
    from dws.dws_user_user_login_td
    where dt='${hiveconf:etl_date}'
      and login_date_last=date_add('${hiveconf:etl_date}',-7)
) churn
join
(
    select
        '${hiveconf:etl_date}' dt,
        count(*) user_back_count
    from
        (
            select
                user_id,
                login_date_last
            from dws.dws_user_user_login_td
            where dt='${hiveconf:etl_date}'
              and login_date_last = '${hiveconf:etl_date}'
        )t1
            join
        (
            select
                user_id,
                login_date_last login_date_previous
            from dws.dws_user_user_login_td
            where dt=date_add('${hiveconf:etl_date}',-1)
        )t2
        on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
)back on churn.dt=back.dt;