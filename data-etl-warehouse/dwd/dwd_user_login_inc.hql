-- DROP TABLE IF EXISTS dwd.dwd_user_login_inc;
CREATE EXTERNAL TABLE if not exists dwd.dwd_user_login_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备ID',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dwd/dwd_user_login_inc/'
TBLPROPERTIES ("orc.compress" = "snappy");



--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dwd层-用户域用户登录事务事实表
-- ********************************************************************

-- 每日装载
insert overwrite table dwd.dwd_user_login_inc partition (dt = '${hiveconf:etl_date}')
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from
(
    select user_id,
            channel,
            province_id,
            version_code,
            mid_id,
            brand,
            model,
            operate_system,
            ts
    from
    (
        select
            common.uid user_id,
            common.ch  channel,
            common.ar  province_id,
            common.vc  version_code,
            common.mid mid_id,
            common.ba  brand,
            common.md  model,
            common.os  operate_system,
            ts,
            row_number() over (partition by common.sid order by ts) rn
        from ods.ods_log_inc
        where dt = '${hiveconf:etl_date}'
          and page is not null
          and common.uid is not null) t1
     where rn = 1
) t2;