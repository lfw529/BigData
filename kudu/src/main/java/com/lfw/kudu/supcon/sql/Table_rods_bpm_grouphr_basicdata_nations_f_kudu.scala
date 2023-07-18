package com.lfw.kudu.supcon.sql

import com.lfw.kudu.supcon.config.Config

object Table_rods_bpm_grouphr_basicdata_nations_f_kudu {
  val source: String =
    s"""
       |create table rods_bpm_grouphr_basicdata_nations_f_kudu (
       id                                  bigint,
       |  version                             bigint comment '版本信息',
       |  create_staff_id                     bigint comment '创建者',
       |  create_time                         string comment '创建时间',
       |  delete_staff_id                     bigint comment '删除者',
       |  delete_time                         string comment '删除时间',
       |  modify_staff_id                     bigint comment '修改者',
       |  modify_time                         string comment '修改时间',
       |  valid                               bigint comment '是否有效',
       |  cid                                 bigint comment '公司id',
       |  lay_no                              bigint comment '层级',
       |  lay_rec                             string comment '层级结构',
       |  parent_id                           bigint comment '上级节点id',
       |  create_department_id                bigint comment '创建部门',
       |  create_position_id                  bigint comment '创建岗位',
       |  effect_staff_id                     bigint comment '生效人',
       |  effect_time                         string comment '生效时间',
       |  oa                                  string comment 'oa字段',
       |  owner_department_id                 bigint comment '所有者部门',
       |  owner_position_id                   bigint comment '所有者主岗',
       |  owner_staff_id                      bigint comment '所有者',
       |  position_lay_rec                    string comment '岗位层级结构',
       |  status                              bigint comment '状态',
       |  table_no                            string comment '单据编号',
       |  code                                string comment '编码',
       |  name                                string comment '地区',
       |  ec_sort                             string ,
       |  sort bigint                         comment '顺序',
       |  deployment_id                       bigint comment '流程id',
       |  process_key                         string comment '流程key',
       |  process_version                     bigint comment '流程版本',
       |  full_path_name                      string comment '层级全路径',
       |  leaf                                bigint comment '是否叶子',
       |  effective_state                     bigint comment '生效状态',
       |  table_info_id                       bigint comment '表单id',
       |  laycode                             bigint comment '级别',
       |  layrelation                         string comment '层级关系',
       |  namerec                             string comment '名称全层级',
       |  param1                              string comment '省份/洲',
       |  param2                              string comment '城市/国家',
       |  param3                              string comment '区县旗',
       |  dpid                                bigint comment '直接父级id',
       |  pinyin                              string comment '名称拼音首字母',
       |  state                               bigint comment '状态',
       |  lay_code                            string comment '级别',
       |  description                         string comment '备注说明',
       |  des                                 string comment '备注说明'
       |)
       |WITH (
       |  'connector.type' = 'kudu',
       |  'kudu.masters' = '${Config.kuduBrokersTest}',
       |  'kudu.table' = 'rods.rods_bpm_grouphr_basicdata_nations_f_kudu',
       |  'kudu.primary-key-columns' = 'id',
       |  'kudu.hash-columns' = 'id'
       |)
       |""".stripMargin

  val query: String =
    s"""
       |select * from rods_bpm_grouphr_basicdata_nations_f_kudu
       |""".stripMargin
}
