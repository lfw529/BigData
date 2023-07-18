package com.lfw.kudu.supcon.sql

import com.lfw.kudu.supcon.config.Config

object Table_rods_bpm_grouphr_base_department_f_kudu {
  val source: String =
    s"""
       |create table rods_bpm_grouphr_base_department_f_kudu (
       |  id                             bigint comment 'id',
       |  code                           string comment '部门编码',
       |  create_staff_id                bigint comment '创建者',
       |  create_time                    string comment '创建时间',
       |  delete_staff_id                bigint comment '删除者',
       |  delete_time                    string comment '删除时间',
       |  description                    string comment '描述',
       |  lay_no                         bigint comment '层级',
       |  lay_rec                        string comment '层级结构',
       |  leaf                           bigint comment '是否叶子',
       |  modify_staff_id                bigint comment '修改者',
       |  modify_time                    string comment '修改时间',
       |  name                           string comment '部门名称',
       |  parent_id                      bigint comment '上级节点id',
       |  sort                           bigint comment '排序',
       |  valid                          bigint comment '是否有效',
       |  version                        bigint comment '版本信息',
       |  cid                            bigint comment '公司id',
       |  staff_id                       bigint comment '部门负责人',
       |  createstaffid                  bigint comment '创建者id',
       |  createtime                     string comment '创建时间',
       |  deletestaffid                  bigint comment '删除者id',
       |  deletetime                     string comment '删除时间',
       |  layno                          bigint comment '层级',
       |  modifystaffid                  bigint comment '修改者id',
       |  modifytime                     string comment '修改时间',
       |  company_id                     bigint comment '公司id',
       |  full_path_name                 string comment '层级全路径',
       |  is_virtual                     bigint comment '是否虚拟部门',
       |  objparame                      bigint comment '自定义字段ope',
       |  objparamd                      bigint comment '自定义字段opd',
       |  objparamc                      bigint comment '自定义字段opc',
       |  objparamb                      bigint comment '自定义字段opb',
       |  objparama                      bigint comment '自定义字段opa',
       |  res_scparame                   string ,
       |  res_scparamd                   string ,
       |  res_scparamc                   string ,
       |  res_scparamb                   string ,
       |  res_scparama                   string ,
       |  charparame                     string ,
       |  charparamd                     string ,
       |  charparamc                     string ,
       |  charparamb                     string comment '自定义字段cpb',
       |  charparama                     string comment '自定义字段cpa',
       |  numberparame                   double ,
       |  numberparamd                   double ,
       |  numberparamc                   double ,
       |  numberparamb                   double comment '自定义字段fpb',
       |  numberparama                   double comment '千分位fpa-lhjtest01',
       |  dateparame                     string ,
       |  dateparamd                     string ,
       |  dateparamc                     string ,
       |  dateparamb                     string ,
       |  dateparama                     string ,
       |  integerparame                  bigint ,
       |  integerparamd                  bigint ,
       |  integerparamc                  bigint ,
       |  integerparamb                  bigint ,
       |  integerparama                  bigint ,
       |  customer_field2                string comment '用于软件公司同步接口',
       |  customer_field1                string comment '用于软件公司同步接口',
       |  sc_nature string,
       |  uuid string  comment '用于软件公司同步接口'
       |)
       |WITH (
       |  'connector.type' = 'kudu',
       |  'kudu.masters' = '${Config.kuduBrokersTest}',
       |  'kudu.table' = 'rods.rods_bpm_grouphr_base_department_f_kudu',
       |  'kudu.primary-key-columns' = 'id',
       |  'kudu.hash-columns' = 'id'
       |)
       |""".stripMargin

  val query: String =
    s"""
      |select * from rods_bpm_grouphr_base_department_f_kudu
      |""".stripMargin
}
