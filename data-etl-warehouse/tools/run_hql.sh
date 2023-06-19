#!/usr/bin/bash

#param 参数

#hive 脚本目录
hive_script=""

#传入时间
etl_time=""

#向前推进时间，默认为1天
back_time="1d"

#执行频率
exec_freq=""

#调度队列设置，默认default队列，根据 yarn 侧配置填写
yarn_queue="BD_OT"

#延迟分钟数
delay=0

#用法提示
help_str="Usage: \n-f xxx.hql \n-t etl_time \n-b back_time(default 1d, m-month d-day h-hour) \n-x exec_freq(default every day, wn-week n in every week(w0-Sunday), dn-day n in every month) \n-q yarn-queue(default BD_OT) \n-d delay(default 0 min)"

#默认向前推进时间单位
back_unit="days"

#默认向前推进值，这里是1天
back_value="1"

#getopts 获取参数匹配
while getopts "f:t:b:x:q:d:h" opt
do  
    case $opt in  
        f) hive_script=$OPTARG
        ;;
        t) etl_time=$OPTARG
        ;;
        b) back_time=$OPTARG
        ;;
        x) exec_freq=$OPTARG
        ;;
        q) yarn_queue=$OPTARG
        ;;
        d) delay=$((10#$OPTARG))
        ;;
        h) echo -e $help_str
           exit 0
        ;;
        \?) echo "Invalid option: -$OPTARG"
            # shellcheck disable=SC2086
            echo -e $help_str
            # shellcheck disable=SC2242
            exit -1
        ;;
  esac  
done  

#check opt
# [ -z string ] 判断string，长度为0则为真
if [[ -z "${hive_script}" || -z "${etl_time}" ]];then
    echo "Hive_script/etl_time should not be empty."
    # shellcheck disable=SC2242
    exit -1
fi

exec_d=`date -d "${etl_time}" "+%d"`
exec_d_ex=${exec_d##*0}
exec_w=`date -d "${etl_time}" "+%w"`
#check exec condition
if [[ -n ${exec_freq} && ${exec_freq} != "d${exec_d}" && ${exec_freq} != "d${exec_d_ex}" && ${exec_freq} != "w${exec_w}" ]];then
    echo "Not in run time and return success. exec_freq=${exec_freq}, exec_d=${exec_d}, exec_w=${exec_w}"
    exit 0
fi

#check back time
# 获取向前推进时间单位：d。back_time: -1：表示取back_time最后一个字符。
back_unit=${back_time: -1}
back_value=$((10#${back_time%${back_unit}*}))
case $back_unit in
    m) back_unit="month"
    ;;
    d) back_unit="day"
    ;;
    h) back_unit="hour"
    ;;
    *) echo "Wrong unit for back_time.(m-month d-day h-hour)"
        # shellcheck disable=SC2242
        exit -1
    ;;
esac

current_date=${etl_time:0:10}
# shellcheck disable=SC2006
etl_time=`date -d "${etl_time} ${back_value} ${back_unit} ago" "+%Y-%m-%d %H:%M:%S"`
etl_time=${etl_time:0:13}":00:00"
etl_date=${etl_time:0:10}
rpt_date=${etl_date}
year=${etl_date:0:4}
month=${etl_date:0:7}
hour=${etl_time:11:2}
simple_month=${etl_date:5:2}
simple_date=${etl_date:8:2}
next_one_hour=`date -d "${etl_time} 1 hour" "+%H"`
next_two_hour=`date -d "${etl_time} 2 hour" "+%H"`
next_two_hour_day=`date -d "${etl_time} 2 hour" "+%F"`
next_two_hour_year=${next_two_hour_day:0:4}
next_two_hour_month=${next_two_hour_day:0:7}
last_day=`date -d "${etl_date} yesterday" "+%F"`
last_one_hour=`date -d "${etl_time} 1 hour ago" "+%H"`
last_one_hour_day=`date -d "${etl_time} 1 hour ago" "+%F"`
last_six_hour=`date -d "${etl_time} 6 hour ago" "+%H"`
last_six_hour_day=`date -d "${etl_time} 6 hour ago" "+%F"`
last_six_hour_month=`date -d "${etl_time} 6 hour ago" "+%Y-%m"`
last_six_hour_year=`date -d "${etl_time} 6 hour ago" "+%Y"`
last_day_year=${last_day:0:4}
last_day_month=${last_day:0:7}
last_month=`date -d "${etl_date} last month" "+%Y-%m"`
tomorrow=`date -d "${etl_date} tomorrow" "+%F"`
tomorrow_year=${tomorrow:0:4}
tomorrow_month=${tomorrow:0:7}
two_days_ago=`date -d "${etl_date} 2 days ago" "+%F"`
seven_days_ago=`date -d "${etl_date} 7 days ago" "+%F"`
three_days_ago=`date -d "${etl_date} 3 days ago" "+%F"`
eight_days_ago=`date -d "${etl_date} 7 days ago" "+%F"`
fourteen_days_ago=`date -d "${etl_date} 13 days ago" "+%F"`
thirty_days_ago=`date -d "${etl_date} 30 days ago" "+%F"`
sixty_days_ago=`date -d "${etl_date} 60 days ago" "+%F"`
ninety_days_ago=`date -d "${etl_date} 90 days ago" "+%F"`
etl_date_no_hyphen=`date -d "${etl_date}" "+%Y%m%d"`
etl_date_no_hyphen_y=`date -d "${etl_date}" "+%y%m%d"`
last_day_no_hyphen=`date -d "${last_day}" "+%Y%m%d"`
month_no_hyphen=`date -d "${etl_date}" "+%Y%m"`
first_day_of_month=${month}"-01"
last_day_of_last_month=`date -d "${first_day_of_month} yesterday" "+%F"`
week_day=`date -d "${etl_date}" "+%w"`

# check retire_day
# shellcheck disable=SC2053
if [[ ${last_day_of_last_month} = ${seven_days_ago} ]];then
	retire_day=9999-99-99
else
	retire_day=${seven_days_ago}
fi;

#delay 
sleep ${delay}m

hive -hiveconf mapreduce.job.queuename=${yarn_queue} \
    -hiveconf hive.exec.reducers.max=40 \
    -hiveconf hive.exec.reducers.bytes.per.reducer=536870912 \
    -hiveconf mapred.job.reuse.jvm.num.tasks=8 \
    -hiveconf mapreduce.input.fileinputformat.split.minsize=33554432 \
    -hiveconf current_date=${current_date} \
    -hiveconf etl_time=${etl_time} \
    -hiveconf etl_date=${etl_date} \
    -hiveconf rpt_date=${rpt_date} \
    -hiveconf year=${year} \
    -hiveconf month=${month} \
    -hiveconf hour=${hour} \
    -hiveconf next_one_hour=${next_one_hour} \
    -hiveconf next_two_hour=${next_two_hour} \
    -hiveconf next_two_hour_day=${next_two_hour_day} \
    -hiveconf next_two_hour_year=${next_two_hour_year} \
    -hiveconf next_two_hour_month=${next_two_hour_month} \
    -hiveconf last_six_hour=${last_six_hour} \
    -hiveconf last_six_hour_day=${last_six_hour_day} \
    -hiveconf last_six_hour_month=${last_six_hour_month} \
    -hiveconf last_six_hour_year=${last_six_hour_year} \
    -hiveconf simple_month=${simple_month} \
    -hiveconf simple_date=${simple_date} \
    -hiveconf last_one_hour=${last_one_hour}\
    -hiveconf last_day=${last_day} \
    -hiveconf last_day_year=${last_day_year} \
    -hiveconf last_day_month=${last_day_month} \
    -hiveconf last_month=${last_month} \
    -hiveconf tomorrow=${tomorrow} \
    -hiveconf tomorrow_year=${tomorrow_year} \
    -hiveconf tomorrow_month=${tomorrow_month} \
    -hiveconf two_days_ago=${two_days_ago} \
    -hiveconf seven_days_ago=${seven_days_ago} \
    -hiveconf three_days_ago=${three_days_ago} \
    -hiveconf eight_days_ago=${eight_days_ago} \
    -hiveconf fourteen_days_ago=${fourteen_days_ago} \
    -hiveconf thirty_days_ago=${thirty_days_ago} \
    -hiveconf sixty_days_ago=${sixty_days_ago} \
    -hiveconf ninety_days_ago=${ninety_days_ago} \
    -hiveconf etl_date_no_hyphen=${etl_date_no_hyphen} \
    -hiveconf etl_date_no_hyphen_y=${etl_date_no_hyphen_y} \
    -hiveconf last_day_no_hyphen=${last_day_no_hyphen} \
    -hiveconf month_no_hyphen=${month_no_hyphen} \
    -hiveconf first_day_of_month=${first_day_of_month} \
    -hiveconf last_day_of_last_month=${last_day_of_last_month} \
    -hiveconf week_day=${week_day} \
    -hiveconf retire_day=${retire_day} \
    -hiveconf last_one_hour_day=${last_one_hour_day} \
    -f ${hive_script}

ret_code=$?
if [[ $ret_code -ne 0 ]];then
    echo "Error when run hive script. $ret_code"
    # shellcheck disable=SC2242
    exit -1
fi

