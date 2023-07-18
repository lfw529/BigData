package com.lfw.kudu.supcon.sql

import com.lfw.kudu.supcon.config.Config

object Table_rods_bpm_grouphr_techcrm_region_contrasts_f_kudu {
  val source: String =
    s"""
       |create table rods_bpm_grouphr_techcrm_region_contrasts_f_kudu (
       |  id                     bigint ,
       |  pmo_staff              bigint comment 'pmo',
       |  is_specialsc           string comment '是否特殊',
       |  version                bigint comment '版本信息',
       |  delete_time            string comment '删除时间',
       |  modify_time            string comment '修改时间',
       |  create_time            string comment '创建时间',
       |  delete_staff_id        bigint comment '删除者',
       |  modify_staff_id        bigint comment '修改者',
       |  create_staff_id        bigint comment '创建者',
       |  valid                  bigint comment '是否有效',
       |  cid                    bigint comment '公司id',
       |  sort                   bigint comment '顺序',
       |  effective_state        bigint comment '生效状态',
       |  process_version        bigint comment '流程版本',
       |  process_key            string comment '流程key',
       |  deployment_id          bigint comment '流程id',
       |  group_id               bigint comment '组id',
       |  status                 bigint comment '状态',
       |  effect_time            string comment '生效时间',
       |  effect_staff_id        bigint comment '生效人',
       |  owner_department_id    bigint comment '所有者部门',
       |  owner_position_id      bigint comment '所有者主岗',
       |  owner_staff_id         bigint comment '所有者',
       |  position_lay_rec       string comment '岗位层级结构',
       |  table_info_id          bigint comment '表单id',
       |  table_no               string comment '单据编号',
       |  create_position_id     bigint comment '创建岗位',
       |  create_department_id   bigint comment '创建部门',
       |  twoleveladd            string comment '二级地址',
       |  threeleveladd          string comment '三级地址',
       |  regiostaff             bigint comment '大区负责人',
       |  region                 bigint comment '大区名称',
       |  oneleveladd            string comment '一级地址',
       |  fourleveladd           string comment '四级地址',
       |  dept                   bigint comment '5s店名称, 5s店名称',
       |  addressall             bigint comment '地址全路径',
       |  deptstaff              bigint comment '5s店负责人',
       |  regionstaff            bigint comment '大区负责人',
       |  sr_staff               bigint comment '大区sr副总',
       |  sec_staff              bigint comment '大区运营副总',
       |  is_special             bigint comment '是否特殊',
       |  fr_staff               bigint comment '大区fr副总',
       |  ar_staff               bigint comment '大区ar副总'
       |)
       |WITH (
       |  'connector.type' = 'kudu',
       |  'kudu.masters' = '${Config.kuduBrokersTest}',
       |  'kudu.table' = 'rods.rods_bpm_grouphr_techcrm_region_contrasts_f_kudu',
       |  'kudu.primary-key-columns' = 'id',
       |  'kudu.hash-columns' = 'id'
       |)
       |""".stripMargin

  val query: String =
    s"""
       |select * from rods_bpm_grouphr_techcrm_region_contrasts_f_kudu
       |""".stripMargin
}
