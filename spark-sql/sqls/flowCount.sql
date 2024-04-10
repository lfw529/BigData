-- 对同一个用来两天相邻上网的数据相差10分钟内的进行rollup

-- 1. 划分窗口运算，lag over
select
  uid,
  start_time,
  end_time,
  flow,
  lag(end_time, 1, start_time) over(partition by uid order by start_time) lag_time
from
  v_user_data


-- 2. 逐行运算，将当前行的起始时间 - 上一行的结束时间 (lag_time)
select
  uid,
  start_time,
  end_time,
  flow,
  if(to_unix_timestamp(start_time, 'yyyy-MM-dd HH:mm:ss') - to_unix_timestamp(lag_time, 'yyyy-MM-dd HH:mm:ss') > 600, 1, 0) flag
from
(
  select
    uid,
    start_time,
    end_time,
    flow,
    lag(end_time, 1, start_time) over(partition by uid order by start_time) lag_time
  from
    v_user_data
)

-- 上面sql的执行结果如下

+---+-------------------+-------------------+----+----+
|uid|         start_time|           end_time|flow|flag|
+---+-------------------+-------------------+----+----+
|  1|2020-02-18 14:20:30|2020-02-18 14:46:30|  20|   0|
|  1|2020-02-18 14:47:20|2020-02-18 15:20:30|  30|   0|
|  1|2020-02-18 15:37:23|2020-02-18 16:05:26|  40|   1|
|  1|2020-02-18 16:06:27|2020-02-18 17:20:49|  50|   0|
|  1|2020-02-18 17:21:50|2020-02-18 18:03:27|  60|   0|
|  2|2020-02-18 14:18:24|2020-02-18 15:01:40|  20|   0|
|  2|2020-02-18 15:20:49|2020-02-18 15:30:24|  30|   1|
|  2|2020-02-18 16:01:23|2020-02-18 16:40:32|  40|   1|
|  2|2020-02-18 16:44:56|2020-02-18 17:40:52|  50|   0|
|  3|2020-02-18 14:39:58|2020-02-18 15:35:53|  20|   0|
|  3|2020-02-18 15:36:39|2020-02-18 15:24:54|  30|   0|
+---+-------------------+-------------------+----+----+

-- 3 将数据按照用户id划分窗口，使用sum over 将flag进行相加，将flag从第一行进行累加，累加到当前行

select
  uid,
  start_time,
  end_time,
  flow,
  sum(flag) over(partition by uid order by start_time rows between unbounded preceding and current row) sum_flag
from
(
  select
    uid,
    start_time,
    end_time,
    flow,
    if(to_unix_timestamp(start_time, 'yyyy-MM-dd HH:mm:ss') - to_unix_timestamp(lag_time, 'yyyy-MM-dd HH:mm:ss') > 600, 1, 0) flag
  from
  (
    select
      uid,
      start_time,
      end_time,
      flow,
      lag(end_time, 1, start_time) over(partition by uid order by start_time) lag_time
    from
      v_user_data
  )
)

-- 上面sql执行后的结果如下
+---+-------------------+-------------------+----+--------+
|uid|         start_time|           end_time|flow|sum_flag|
+---+-------------------+-------------------+----+--------+
|  1|2020-02-18 14:20:30|2020-02-18 14:46:30|  20|       0|
|  1|2020-02-18 14:47:20|2020-02-18 15:20:30|  30|       0|
|  1|2020-02-18 15:37:23|2020-02-18 16:05:26|  40|       1|
|  1|2020-02-18 16:06:27|2020-02-18 17:20:49|  50|       1|
|  1|2020-02-18 17:21:50|2020-02-18 18:03:27|  60|       1|
|  2|2020-02-18 14:18:24|2020-02-18 15:01:40|  20|       0|
|  2|2020-02-18 15:20:49|2020-02-18 15:30:24|  30|       1|
|  2|2020-02-18 16:01:23|2020-02-18 16:40:32|  40|       2|
|  2|2020-02-18 16:44:56|2020-02-18 17:40:52|  50|       2|
|  3|2020-02-18 14:39:58|2020-02-18 15:35:53|  20|       0|
|  3|2020-02-18 15:36:39|2020-02-18 15:24:54|  30|       0|
+---+-------------------+-------------------+----+--------+

-- 4 按照uid 和 sum_flag 进行分组聚合
select
  uid,
  min(start_time) start_time,
  max(end_time) end_time,
  sum(flow) flow
from
(
  select
    uid,
    start_time,
    end_time,
    flow,
    sum(flag) over(partition by uid order by start_time rows between unbounded preceding and current row) sum_flag
  from
  (
    select
      uid,
      start_time,
      end_time,
      flow,
      if(to_unix_timestamp(start_time, 'yyyy-MM-dd HH:mm:ss') - to_unix_timestamp(lag_time, 'yyyy-MM-dd HH:mm:ss') > 600, 1, 0) flag
    from
    (
      select
        uid,
        start_time,
        end_time,
        flow,
        lag(end_time, 1, start_time) over(partition by uid order by start_time) lag_time
      from
        v_user_data
    )
  )
)
group by uid, sum_flag