package com.lfw.kudu.supcon.sql

import com.lfw.kudu.supcon.config.Config

object Table_rods_dmp_bigdata_customer_intelligence_system_t_ci_role_data_perm_f_kudu {
  val source: String =
    s"""
       |create table rods_dmp_bigdata_customer_intelligence_system_t_ci_role_data_perm_f_kudu (
       |  id               bigint comment 'id',
       |  role_id          bigint comment '角色id',
       |  data_type        bigint comment '类型：1：操作权限  2：查询权限',
       |  resource_id      bigint comment '资源id',
       |  permissions      string comment '操作权限集(一条数据可能会涉及到多个操作)',
       |  data_menu_id     bigint comment '所属菜单 1：最新项目&未处理项目 2：新客户项目 3：客户资讯 4：园区资讯 5：政策资讯 6：客户画像',
       |  tab_type         bigint comment 'tab类型 1：大区 2：省份 3：5s店 4：客户'
       |)
       |WITH (
       |  'connector.type' = 'kudu',
       |  'kudu.masters' = '${Config.kuduBrokers}',
       |  'kudu.table' = 'rods.rods_dmp_bigdata_customer_intelligence_system_t_ci_role_data_perm_f_kudu',
       |  'kudu.primary-key-columns' = 'id',
       |  'kudu.hash-columns' = 'id'
       |)
       |""".stripMargin
}
