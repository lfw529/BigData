#!/usr/bin/bash

DIM_DB=dim                  # 正式维表库
ODS_DB=ods                  # 正式ods库
ODS_TMP_DB=ods_tmp          # 临时ods库
TMP_DB_PARTITION_KEEP=7     # 临时hive库保留分区数

# 读取数据库配置
MYSQL_CONF=/opt/dw/data-etl-warehouse/etc/mysql.json

#获取文件的绝对路径并展示
WORK_DIR=$(cd `dirname $0`; pwd)
cd ${WORK_DIR}

#param 相关参数
#文件名
tbl_conf=""
#传入当天的日期
etl_date=""
#向前推进一天, 默认为1
back_days=1
#map任务个数
map_num=1
#执行频率
exec_freq=""
#延迟分钟数
delay=0
help_str="Usage: -f tbl_conf -t etl_date -b back_days(default 1) -m map_num(default 1, 1-30) -x exec_freq(default every day, wn-week n in every week(w0-Sunday), dn-day n in every month)-d delay(default 0 min)"


tbl_conf="/opt/dw/data-etl-warehouse/ods/tbl_conf/ods_order_info_inc.conf"
etl_date=2022-06-09
# 具体书写案例：
#/opt/dw/data-etl-warehouse/tools/sqoop_import.sh -f /opt/dw/data-etl-warehouse/ods/tbl_conf/ods_order_info_inc.conf -t 2022-06-09 -m 10

#get opt 获取相关参数 按顺序切分 | getopts 行参数解析匹配函数 | $OPTARG:对应参数 | 10# 十进制
#getopts 有两个参数，第一个参数是一个字符串，包括字符和 ":"。每一个字符都是一个有效选项(option)，如果字符后面带有":"，表示这个选项有自己的 argument，argument 保存在内置变量OPTARG中

while getopts "f:t:b:m:x:d:h" opt;
do
    case $opt in
    # -f 之后的参数： /opt/dw/data-etl-warehouse/ods/tbl_conf/ods_order_info_inc.conf
    f)
      tbl_conf=$OPTARG
      ;;
    # -t 之后的参数：2021-04-22
    t)
      etl_date=$OPTARG
      ;;
    b)
      back_days=$((10#$OPTARG))
      ;;
    # -m 之后的参数：10
    m)
      map_num=$((10#$OPTARG))
      ;;
    x)
      exec_freq=$OPTARG
      ;;
    d)
      delay=$((10#$OPTARG))
      ;;
    h)
      echo "$help_str"
      exit 0
      ;;
    \?)
      echo "Invalid option: -$OPTARG"
      echo "$help_str"
      # shellcheck disable=SC2242
      exit -1
      ;;
    esac
done

# 检查文件名 [tbl_conf] 是否为空，导表日期 [etl_date] 是否为空 | -z 判断字符串长度是否为0，为0为真
# [] 与 [[]] 的区别：使用 [[ ... ]] 条件判断结构，而不是 [ ... ]，能够防止脚本中的许多逻辑错误。&&、||、< > 操作符能够正常存在于 [[ ]] 条件判断结构中，但是如果出现在 [ ] 结构中的话，会报错。
# 可以直接使用 if [[ $a != 1 && $a != 2 ]], 如果不使用双括号, 则为 if [ $a -ne 1] && [ $a != 2 ] 或者 if [ $a -ne 1 -a $a != 2 ]。
if [[ -z "${tbl_conf}" || -z "${etl_date}" ]]; then
  echo "Table/etl_date should not be empty."
  # shellcheck disable=SC2242
  exit -1
fi

#check exec condition 检查执行条件
exec_d=$(date -d "${etl_date}" "+%d") #显示当前日期： "%d" 表示日期，比如说：输入 "2021-04-22" --> 22
#file=/dir1/dir2/dir3/my.file.txt
#可以用${ }分别替换得到不同的值：
# ${file#*/}：删掉第一个 / 及其左边的字符串：dir1/dir2/dir3/my.file.txt
# ${file##*/}：删掉最后一个 / 及其左边的字符串：my.file.txt
# ${file#*.}：删掉第一个 . 及其左边的字符串：file.txt
# ${file##*.}：删掉最后一个 . 及其左边的字符串：txt
# ${file%/*}：删掉最后一个  / 及其右边的字符串：/dir1/dir2/dir3
# ${file%%/*}：删掉第一个 / 及其右边的字符串：(空值)
# ${file%.*}：删掉最后一个 . 及其右边的字符串：/dir1/dir2/dir3/my.file
# ${file%%.*}：删掉第一个 . 及其右边的字符串：/dir1/dir2/dir3/my
#记忆的方法为：
# #是去掉左边（键盘上 # 在 $ 的左边）
# %是去掉右边（键盘上 % 在 $ 的右边）
#单一符号是最小匹配；两个符号是最大匹配
# ${file:0:5}：提取最左边的 5 个字节：/dir1
# ${file:5:5}：提取第 5 个字节右边的连续5个字节：/dir2
#也可以对变量值里的字符串作替换：
# ${file/dir/path}：将第一个 dir 替换为 path：/path1/dir2/dir3/my.file.txt
# ${file//dir/path}：将全部 dir 替换为 path：/path1/path2/path3/my.file.txt
exec_d_ex=${exec_d##*0}               #从右往左出现第一个0的左边部分全部截掉,包括0。比如：06 --> 6
exec_w=$(date -d "${etl_date}" "+%w") #判断 weekday,该日期在一周中的第几天，星期一为第1天，星期六为第6天

# 判断执行日期是否与执行频率匹配
# 如果执行频率的字符串长度大于0 且 执行频率 ！= dn[d03] 且 执行频率 ！= dn[d3] 且 执行频率 ！= wn[w6]     比如日期：2023-06-03
if [[ -n ${exec_freq} && ${exec_freq} != "d${exec_d}" && ${exec_freq} != "d${exec_d_ex}" && ${exec_freq} != "w${exec_w}" ]];then #拼接 d1-d30 与 exec_freq 匹配
  echo "Not in run time and return success. exec_freq=${exec_freq}, exec_d=${exec_d}, exec_w=${exec_w}"
  exit 0
fi

# 限制设置 map 的个数为 1 - 30 个之间，超过报错
if [[ ${map_num} -gt 30 || ${map_num} -lt 1 ]];then
  echo "map_num limit to 1 - 30."
  # shellcheck disable=SC2242
  exit -1
fi

# 时间变量声明
# 今天的时间
today=${etl_date}
# ETL 的时间，可以自主设置，默认是当前日期 -1。 "%F" 表示完整日期格式，等价于 %Y-%m-%d
etl_date=$(date -d "${etl_date} ${back_days} days ago" "+%F")
# ETL 的时间，带时分秒
etl_datetime="${etl_date} 00:00:00"
# ETL 后一天时间，带时分秒
next_datetime="${etl_date} 23:59:59"
#next_datetime=`date -d "${etl_date} tomorrow" "+%F"`" 00:00:00"

# 以当前时间为基准，自 UTC 时间 1970-01-01 00:00:00 以来所经过的秒数
etl_unix=$(date -d "${etl_date}" "+%s")
# 以当前时间的后一天为基准，自 UTC 时间 1970-01-01 00:00:00 以来所经过的秒数
next_unix=$(date -d "${next_datetime}" "+%s")
# 从文件名中截取表名，例如：ods_order_info_inc.conf  --> ods_order_info_inc
tbl_name=$(echo ${tbl_conf##*/} | cut -d '.' -f 1)
# 获取 etl 年月日，格式为：yyyymmdd --> 20230603
yyyymmdd=$(date -d "${etl_date}" "+%Y%m%d")
# 获取 etl 年月，格式为：yyyymm --> 202306
yyyymm=$(date -d "${etl_date}" "+%Y%m")

#delay 延迟分钟数 [默认不延迟]
sleep ${delay}m

#-----------------------------------------------配置读取2------------------------------------------#
# 声明任务开始时间为 当前时间
start_time=$(date "+%Y-%m-%d %H:%M:%S")
#run sqoop import with param: /opt/dw/data-etl-warehouse/ods/tbl_conf/ods_order_info_inc.conf 2023-06-03 1 10
echo "run sqoop import with param: ${tbl_conf} ${etl_date} ${back_days} ${map_num}"
# 从文件中一行一行的读取表配置，并对每一行配置循环做如下操作
while read line || [[ -n "$line" ]]
do
    # 获取当前行的所涉及的时间参数，并作相应的替换。例如：date_format(createdAt, "%Y-%m-%d")={etl_date} --> date_format(createdAt, "%Y-%m-%d")='2023-06-03'
    line=$(echo ${line} | sed "s/{etl_date}/'${etl_date}'/g" | sed "s/{etl_unix}/${etl_unix}/g" | sed "s/{next_unix}/${next_unix}/g" | sed "s/{etl_datetime}/'${etl_datetime}'/g" | sed "s/{next_datetime}/'${next_datetime}'/g" | sed "s/{today}/'${today}'/g" | sed "s/{yyyymmdd}/${yyyymmdd}/g" | sed "s/{yyyymm}/${yyyymm}/g")
    # 按照 '|' 进行切割，分割出该行的属性标签[取 | 左边]。例如：mysql_tbl|order --> mysql_tbl。
    tag=$(echo ${line} | cut -d "|" -f 1)
    # 按照 '|' 进行切割，分割出该行的属性标签[取 | 右边]。例如：mysql_tbl|order --> order。
    value=$(echo ${line} | cut -d "|" -f 2)
    # 声明变量并赋值的过程
    case ${tag} in
        "db_inst") db_inst=${value}
        ;;
        "mysql_tbl") mysql_tbl=${value}
        ;;
        "mysql_cols") mysql_cols=${value}
        ;;
        "mysql_cond") mysql_cond=${value}
        ;;
        "is_partition") is_partition=${value}
        ;;
    esac
done < ${tbl_conf} # 从该目录下的文件中读取文件内容

# 选择 ods 层的表，对接 ods 库
if [ "${tbl_name:0:3}" = 'ods' ];then
    hive_db=${ODS_DB}
    hive_tmp_db=${ODS_TMP_DB}
else  # 这边目前用不上
    hive_db=${DIM_DB}
fi

# 全量导入条件判断，如果为空则为全量导入
if [[ "${mysql_cond}" == "" ]];then
    mysql_cond="1 = 1"
fi

# 检查库名信息是否为空
if [[ -z "${db_inst}" ]];then
    echo "Empty db info!"
    # shellcheck disable=SC2242
    exit -1
fi

# 声明数据库数组，(): 表示数组
db_inst_arr=(${db_inst})
# mysql配置文件
db_mysql=${MYSQL_CONF}

hive -e "ALTER TABLE ${hive_tmp_db}.${tbl_name} DROP IF EXISTS PARTITION (dt = '${etl_date}')"

overwrite="--hive-overwrite"

# 遍历解析配置并赋值
# shellcheck disable=SC2068
for db_inst_i in ${db_inst_arr[*]}  #[@] 将该字符串转换为数组然后进行循环遍历
# 定义数组：array_name=(li wang xiang zhang) [小括号做边界、使用空格分离]
# 单独定义数组的元素：array_para[0]="w"; array_para[3]="s" [定义时下标不连续也可以]
# 赋值数组元素：array_name[0]="zhao";
# 获取数组元素：
  # array_name[0]="li"
  # array_name[3]="zhang"
  # echo ${array_name[0]} # 输出"li"
  # echo ${array_name[1]} # 输出" "
  # echo ${array_name[3]} # 输出"zhang"
  # echo ${array_name[@]} # 输出"li zhang" 输出数组所有元素，没有元素的下标省略
# 取得元素个数：${#array_name[@]} 或者 ${#array_name}
# 取得单个元素长度：${#array_name[1]}

do
    # 判断数据库名是否为空
    if [[ -z "${db_inst_i}" ]];then
        echo "Empty db_inst"
        # shellcheck disable=SC2242
        exit -1
    fi

    # jq 学习教程：http://alingse.github.io/jq-manual-cn/manual/v1.5/ 或者 https://jqlang.github.io/jq/manual/
    # jq 安装命令： yum -y install jq
    db_info=`cat ${db_mysql} | jq ". | map(select(.InstanceName == \"${db_inst_i:0:-1}\"))[0]"`         #查询到 map 是遍历器，select 是命中目标，获取第一个 db_info 的信息
    mysql_ip=`echo ${db_info} | jq '.Server' | sed s/\"//g`               # 提取服务器IP/域名，并去掉双引号
    mysql_port=`echo ${db_info} | jq '.Port' | sed s/\"//g`               # 提取应用端口号，并去掉双引号
    mysql_db=`echo ${db_info} | jq '.DB' | sed s/\"//g`                   # 提取库名，并去掉双引号
    connect_str="jdbc:mysql://${mysql_ip}:${mysql_port}/${mysql_db}?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT"  # 拼接 sql-url
    username=`echo ${db_info} | jq '.UserID' | sed s/\"//g`               # 提取用户名，并去掉双引号
    passwd=`echo ${db_info} | jq '.Password' | sed s/\"//g`               # 提取密码，并去掉双引号

    #sqoop 导入至tmp库
    #tmp库使用dt分区，保留最近几天的导入数据
    echo "dump data from ${db_inst_i:0:-1} ${connect_str} ..."
    echo ${mysql_tbl}
    echo ${mysql_cond}

     echo sqoop import \
        -Dmapreduce.job.queuename=root.default \
        --connect ${connect_str} \
        --username ${username} \
        --password ${passwd} \
        --table "${mysql_tbl:0:-1}" \
        --columns ${mysql_cols:0:-1} \
        --where ${mysql_cond:0:-1} \
        --hive-table ${hive_tmp_db}.${tbl_name} \
        --hive-partition-key dt \
        --hive-partition-value ${etl_date} \
        --hive-delims-replacement '\001' \
        --null-string ''\
        --null-non-string '' \
        --delete-target-dir \
        --target-dir "/user/sqoop/import_tmp/${tbl_name}/" \
        --hive-import ${overwrite} \
        --direct \
        -num-mappers ${map_num}

    ret_code=$?
    if [[ $ret_code -ne 0 ]];then
        echo "Error when run sqoop import. $ret_code"
        rm -f ${WORK_DIR}/${mysql_tbl}.java
        # shellcheck disable=SC2242
        exit -1
    fi
    overwrite=""
done


# hive 命令将tmp库导入正式库
# 大部分 ods 表使用 year month day 三级分区，是否分区在配置文件中设置
year=${etl_date:0:4}                      #取年
month=${etl_date:0:7}                     #取月
if [ "${is_partition}" = 'true' ];then    #判断是否分区
    partition_cond=" partition(year='${year}', month='${month}', day='${etl_date}')"   #分区条件赋值
fi

#调用 hive 执行语句
echo "set hive.support.quoted.identifiers=none;
         insert overwrite table ${hive_db}.${tbl_name}${partition_cond}
         select \`(dt)?+.+\` from ${hive_tmp_db}.${tbl_name} where dt='${etl_date}'"




##校验 mysql 和 hive 两边数据条数
##get mysql conf
#db_info=`cat ${db_mysql} | jq ". | map(select(.InstanceName == \"${db_inst}\"))[0]"`  #数据库信息
#conn_host=`echo ${db_info} | jq '.Server' | sed s/\"//g`                              #连接主机
#conn_port=`echo ${db_info} | jq '.Port' | sed s/\"//g`                                #连接端口
#conn_db=`echo ${db_info} | jq '.DB' | sed s/\"//g`                                    #连接数据库
#username=`echo ${db_info} | jq '.UserID' | sed s/\"//g`                               #用户名
#passwd=`echo ${db_info} | jq '.Password' | sed s/\"//g`                               #密码
#if [ -n "${mysql_cond}" ]; then                                                       #判断条件
#    mysql_cond="where "${mysql_cond}
#fi
#
#mysql_num=$(mysql -h${conn_host} -P${conn_port} -u"${username}" -p"${passwd}" ${conn_db} -e "select count(*) as cnt from \`${mysql_tbl}\` ${mysql_cond}")
#
#hive_num=`hive -e "select count(*) as cnt from ${hive_tmp_db}.${tbl_name} where dt = '${etl_date}'"`
#
#if [ ${mysql_num:4} = ${hive_num} ];then is_match='true'; else is_match='false'; fi
#
#end_time=`date "+%Y-%m-%d %H:%M:%S"`
#
#db_info=`cat ${MYSQL_CONF} | jq ". | map(select(.InstanceName == \"report\"))[0]"`
#mysql_ip=`echo ${db_info} | jq '.Server' | sed s/\"//g`
#mysql_port=`echo ${db_info} | jq '.Port' | sed s/\"//g`
#username=`echo ${db_info} | jq '.UserID' | sed s/\"//g`
#passwd=`echo ${db_info} | jq '.Password' | sed s/\"//g`
#
#mysql -h${mysql_ip} -P${mysql_port} -u${username} -p${passwd} -e "use report; insert into import_result (table_name, etl_date, start_time, end_time, src_num, hive_num, status, src_type) VALUES ('${tbl_name}', '${etl_date}', '${start_time}', '${end_time}', ${mysql_num:4}, ${hive_num}, '${is_match}', 0)"
#
#rm -f ${WORK_DIR}/${mysql_tbl}.java