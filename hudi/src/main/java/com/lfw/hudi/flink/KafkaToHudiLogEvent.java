package com.lfw.hudi.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 基于Flink SQL Connector实现：实时消费Topic中数据，转换处理后，实时存储Hudi表中
 */
public class KafkaToHudiLogEvent {
    public static void main(String[] args) {
        //System.setProperty("HADOOP_USER_NAME", "lfw");

        //1.获取表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(30000);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //2.创建输入表，TODO: 从 kafka 消费数据
        tableEnv.executeSql(
                "CREATE TABLE dwd_log_event (\n" +
                        "  package_id INT,\n" +
                        "  user_id BIGINT,\n" +
                        "  platform string,\n" +
                        "  platform_version string,\n" +
                        "  device_model_type string,\n" +
                        "  device_model_version string,\n" +
                        "  device_manufacturer string,\n" +
                        "  device_id string,\n" +
                        "  user_mac string,\n" +
                        "  net_type string,\n" +
                        "  network_operator string,\n" +
                        "  app_name string,\n" +
                        "  app_version string,\n" +
                        "  app_id string,\n" +
                        "  client_version string,\n" +
                        "  uuid string,\n" +
                        "  first_visit_timestamp string,\n" +
                        "  log_date_time string,\n" +
                        "  pv_timestamp BIGINT,\n" +
                        "  c_timestamp BIGINT,\n" +
                        "  path string,\n" +
                        "  query_params string,\n" +
                        "  query_sampshare string,\n" +
                        "  referrer string,\n" +
                        "  mtma string,\n" +
                        "  mtmb string,\n" +
                        "  session_id string,\n" +
                        "  ec string,\n" +
                        "  ea string,\n" +
                        "  el string,\n" +
                        "  ev string,\n" +
                        "  client_ip string,\n" +
                        "  country string,\n" +
                        "  province string,\n" +
                        "  city string,\n" +
                        "  isp string,\n" +
                        "  user_agent string,\n" +
                        "  guid string,\n" +
                        "  campaign_name string,\n" +
                        "  campaign_source string,\n" +
                        "  campaign_medium string,\n" +
                        "  campaign_term string,\n" +
                        "  campaign_content string,\n" +
                        "  campaign_id string,\n" +
                        "  s_timestamp bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'dwd_log_event',\n" +
                        "  'properties.bootstrap.servers' = '10.18.180.21:9092,10.18.180.22:9092,10.18.180.23:9092',\n" +
                        "  'properties.group.id' = 'dev_test',\n" +
                        "  'scan.startup.timestamp-millis' = '1666076400000',\n" +
                        "  'scan.startup.mode' = 'timestamp',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );

        //3.定义输出表，TODO：数据保存到Hudi表中
        tableEnv.executeSql(
                "CREATE TABLE dwd_log_event_hudi (\n" +
                        "    package_id INT,\n" +
                        "    user_id BIGINT,\n" +
                        "    platform string,\n" +
                        "    platform_version string,\n" +
                        "    device_model_type string,\n" +
                        "          device_model_version string,\n" +
                        "          device_manufacturer string,\n" +
                        "          device_id string,\n" +
                        "          user_mac string,\n" +
                        "          net_type string,\n" +
                        "          network_operator string,\n" +
                        "          app_name string,\n" +
                        "          app_version string,\n" +
                        "          app_id string,\n" +
                        "          client_version string,\n" +
                        "          uuid string,\n" +
                        "          first_visit_timestamp string,\n" +
                        "          log_date_time string,\n" +
                        "          pv_timestamp BIGINT,\n" +
                        "          c_timestamp BIGINT,\n" +
                        "          path string,\n" +
                        "          query_params string,\n" +
                        "          query_sampshare string,\n" +
                        "          referrer string,\n" +
                        "          mtma string,\n" +
                        "          mtmb string,\n" +
                        "          session_id string,\n" +
                        "          ec string,\n" +
                        "          ea string,\n" +
                        "          el string,\n" +
                        "          ev string,\n" +
                        "          client_ip string,\n" +
                        "          country string,\n" +
                        "          province string,\n" +
                        "          city string,\n" +
                        "          isp string,\n" +
                        "          user_agent string,\n" +
                        "          guid string,\n" +
                        "          campaign_name string,\n" +
                        "          campaign_source string,\n" +
                        "          campaign_medium string,\n" +
                        "          campaign_term string,\n" +
                        "          campaign_content string,\n" +
                        "          campaign_id string,\n" +
                        "          s_timestamp bigint,\n" +
                        "          hours       string comment '小时',\n" +
                        "          days        string comment '日期'\n" +
                        "        )\n" +
                        "        PARTITIONED BY (days)\n" +
                        "WITH (\n" +
                        "  'connector' = 'hudi',\n" +
                        "  'path' = 'oss://echoing-emr/hudi/dwd.db/dwd_log_event1',\n" +
                        "  'table.type' = 'COPY_ON_WRITE',\n" +
                        "  'read.tasks' = '1',\n " +
                        "  'write.tasks' = '6',\n " +
                        "  'compaction.tasks' = '6',\n " +
                        "  'write.precombine' = 'true',\n " +
                        "  'index.state.ttl' = '0',\n " +
                        "  'write.precombine.field' = 's_timestamp',\n" +
                        "  'hoodie.metrics.on' = 'true', \n" +
                        "  'hoodie.metrics.reporter.type' = 'PROMETHEUS_PUSHGATEWAY', \n" +
                        "  'hoodie.metrics.pushgateway.host' = 'prometheus-pushgateway.monitor.svc', \n" +
                        "  'hoodie.metrics.pushgateway.port' = '9091', \n" +
                        "  'hoodie.metrics.pushgateway.delete.on.shutdown' = 'false', \n" +
                        "  'hoodie.metrics.pushgateway.job.name' = 'mattock_hudi_logevent', \n" +
                        "  'hoodie.metrics.pushgateway.random.job.name.suffix' = 'false', \n" +
                        "  'hive_sync.enable' = 'true',\n" +
                        "  'hive_sync.mode' = 'jdbc',\n" +
                        "  'hive_sync.metastore.uris' = 'thrift://10.20.221.242:9083', \n" +
                        "  'hive_sync.jdbc_url' = 'jdbc:hive2://10.20.221.242:10000', \n" +
                        "  'hive_sync.table'= 'dwd_log_event', \n" +
                        "  'hive_sync.db' = 'hudi_dwd', \n" +
                        "  'hoodie.datasource.write.recordkey.field' = 'guid' \n" +
                        ")"
        );

        //5.通过子查询方式，将数据写入输出表
        tableEnv.executeSql(
                "INSERT INTO dwd_log_event_hudi\n" +
                        "        SELECT package_id,user_id,platform,platform_version,device_model_type,device_model_version,device_manufacturer,\n" +
                        "          device_id,user_mac,net_type,network_operator,app_name,app_version,app_id,client_version,uuid,\n" +
                        "          first_visit_timestamp,log_date_time,pv_timestamp,c_timestamp,path,query_params,query_sampshare,referrer,\n" +
                        "          mtma, mtmb, session_id, ec, ea, el,ev,client_ip ,country ,province ,city ,isp ,user_agent ,guid ,campaign_name ,campaign_source ,\n" +
                        "          campaign_medium,campaign_term,campaign_content,campaign_id, s_timestamp, date_format(from_unixtime(s_timestamp),'HH'), date_format(from_unixtime(s_timestamp),'yyyy/MM/dd')\n" +
                        "        FROM dwd_log_event"
        );

    }
}