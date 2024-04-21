-- DROP TABLE IF EXISTS dwd.dwd_user_register_inc;
CREATE EXTERNAL TABLE if not exists dwd.dwd_user_register_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `create_time`    STRING COMMENT '注册时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备ID',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dwd/dwd_user_register_inc/'
TBLPROPERTIES ("orc.compress" = "snappy");



--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dwd层-流量域页面浏览事务事实表
-- ********************************************************************

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
-- insert overwrite table dwd.dwd_user_register_inc partition(dt)
-- select
--     ui.user_id,
--     date_format(create_time,'yyyy-MM-dd') date_id,
--     create_time,
--     channel,
--     province_id,
--     version_code,
--     mid_id,
--     brand,
--     model,
--     operate_system,
--     date_format(create_time,'yyyy-MM-dd')
-- from
-- (
--     select
--         data.id user_id,
--         data.create_time
--     from ods.ods_user_info_inc
--     where dt='2022-06-08'
--       and type='bootstrap-insert'
-- )ui
-- left join
-- (
--     select
--         common.ar province_id,
--         common.ba brand,
--         common.ch channel,
--         common.md model,
--         common.mid mid_id,
--         common.os operate_system,
--         common.uid user_id,
--         common.vc version_code
--     from ods.ods_log_inc
--     where dt='2022-06-08'
--       and page.page_id='register'
--       and common.uid is not null
-- )log on ui.user_id=log.user_id;


-- 每日装载
insert overwrite table dwd.dwd_user_register_inc partition(dt='${hiveconf:etl_date}')
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
(
    select
        data.id user_id,
        data.create_time
    from ods.ods_user_info_inc
    where dt='${hiveconf:etl_date}'
      and type='insert'
)ui
left join
(
    select
        common.ar province_id,
        common.ba brand,
        common.ch channel,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code
    from ods.ods_log_inc
    where dt='${hiveconf:etl_date}'
      and page.page_id='register'
      and common.uid is not null
)log on ui.user_id=log.user_id;