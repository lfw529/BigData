-- DROP TABLE IF EXISTS dws.dws_user_user_login_td;
CREATE EXTERNAL TABLE if not exists dws.dws_user_user_login_td
(
    `user_id`          STRING COMMENT '用户ID',
    `login_date_last`  STRING COMMENT '历史至今末次登录日期',
    `login_date_first` STRING COMMENT '历史至今首次登录日期',
    `login_count_td`   BIGINT COMMENT '历史至今累计登录次数'
) COMMENT '用户域用户粒度登录历史至今汇总表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
TBLPROPERTIES ('orc.compress' = 'snappy');



--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dws层-用户域用户粒度登录历史至今汇总表
-- ********************************************************************

-- 首日装载
insert overwrite table dws.dws_user_user_login_td partition (dt = '2022-06-08')
select
    u.id                                                         user_id,
    nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')) login_date_last,
    date_format(create_time, 'yyyy-MM-dd')                       login_date_first,
    nvl(login_count_td, 1)                                       login_count_td
from
(
    select
        id,
        create_time
    from dim.dim_user_zip
    where dt = '9999-12-31'
) u
left join
(
    select
        user_id,
        max(dt)  login_date_last,
        count(*) login_count_td
    from dwd.dwd_user_login_inc
    group by user_id
) l on u.id = l.user_id;



-- 每日装载
insert overwrite table dws.dws_user_user_login_td partition (dt = '${hiveconf:etl_date}')
select
    nvl(old.user_id, new.user_id)                                        user_id,
    if(new.user_id is null, old.login_date_last, '${hiveconf:etl_date}')           login_date_last,
    if(old.login_date_first is null, '${hiveconf:etl_date}', old.login_date_first) login_date_first,
    nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0)              login_count_td
from
(
    select
        user_id,
        login_date_last,
        login_date_first,
        login_count_td
    from dws.dws_user_user_login_td
    where dt = date_add('${hiveconf:etl_date}', -1)
) old
full outer join
(
    select user_id,
        count(*) login_count_1d
    from dwd.dwd_user_login_inc
    where dt = '${hiveconf:etl_date}'
    group by user_id
) new on old.user_id = new.user_id;