-- DROP TABLE IF EXISTS dim.dim_user_zip;
CREATE EXTERNAL TABLE if not exists dim.dim_user_zip
(
    `id`           STRING COMMENT '用户ID',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户维度表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dim/dim_user_zip/'
TBLPROPERTIES ('orc.compress' = 'snappy');


--SQL--
-- ********************************************************************
-- Author: lfw
-- CreateTime: 2023-10-04 18:01:52
-- Comment: dim层-用户维度
-- ********************************************************************

-- 首日导入
-- insert overwrite table dim.dim_user_zip partition (dt = '9999-12-31')
-- select data.id,
--        concat(substr(data.name, 1, 1), '*')                name,
--        if(data.phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
--           concat(substr(data.phone_num, 1, 3), '*'), null) phone_num,
--        if(data.email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
--           concat('*@', split(data.email, '@')[1]), null)   email,
--        data.user_level,
--        data.birthday,
--        data.gender,
--        data.create_time,
--        data.operate_time,
--        '2022-06-08'                                        start_date,
--        '9999-12-31'                                        end_date
-- from ods.ods_user_info_inc
-- where dt = '2022-06-08' and type = 'bootstrap-insert';


-- 每日导入
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim.dim_user_zip partition (dt)
select id,
       name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       start_date,
       if(rn = 2, date_sub('${hiveconf:etl_date}', 1), end_date) as end_date,
       if(rn = 1, '9999-12-31', date_sub('${hiveconf:etl_date}', 1)) dt
from
(
    select
        id,
        name,
        phone_num,
        email,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        start_date,
        end_date,
        row_number() over (partition by id order by start_date desc) rn
    from
    (
        select id,
             name,
             phone_num,
             email,
             user_level,
             birthday,
             gender,
             create_time,
             operate_time,
             start_date,
             end_date
        from dim.dim_user_zip
        where dt = '9999-12-31'
        union
        select
            id,
            concat(substr(name, 1, 1), '*')                name,
            if(phone_num regexp
            '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
            concat(substr(phone_num, 1, 3), '*'), null) phone_num,
            if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
            concat('*@', split(email, '@')[1]), null)   email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            '${hiveconf:etl_date}'                                   start_date,
            '9999-12-31'                                   end_date
        from
        (
            select
                data.id,
                data.name,
                data.phone_num,
                data.email,
                data.user_level,
                data.birthday,
                data.gender,
                data.create_time,
                data.operate_time,
                row_number() over (partition by data.id order by ts desc) rn
            from ods.ods_user_info_inc
            where dt = '${hiveconf:etl_date}'
        ) t1
        where rn = 1
    ) t2
) t3;