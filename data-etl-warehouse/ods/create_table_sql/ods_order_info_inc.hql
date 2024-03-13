DROP TABLE IF EXISTS ods_tmp.ods_order_info_inc;
CREATE TABLE if not exists ods_tmp.ods_order_info_inc
(
    `id` bigint COMMENT '编号',
    `consignee` string COMMENT '收货人',
    `consignee_tel` string COMMENT '收件人电话',
    `total_amount` double COMMENT '总金额',
    `order_status` string COMMENT '订单状态',
    `user_id` bigint COMMENT '用户id',
    `payment_way` string COMMENT '付款方式',
    `delivery_address` string COMMENT '送货地址',
    `order_comment` string COMMENT '订单备注',
    `out_trade_no` string COMMENT '订单交易编号（第三方支付用)',
    `trade_body` string COMMENT '订单描述(第三方支付用)',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `expire_time` string COMMENT '失效时间',
    `process_status` string COMMENT '进度状态',
    `tracking_no` string COMMENT '物流单编号',
    `parent_order_id` string COMMENT '父订单编号',
    `img_url` string COMMENT '图片链接',
    `province_id` bigint COMMENT '省份id',
    `activity_reduce_amount` double COMMENT '活动减免金额',
    `coupon_reduce_amount` double COMMENT '优惠券减免金额',
    `original_total_amount` double COMMENT '原始总金额',
    `feight_fee` double COMMENT '运费金额',
    `feight_fee_reduce` double COMMENT '运费减免金额',
    `refundable_time` string COMMENT '可退款时间（签收后30天）'
) COMMENT '临时表-订单表'
PARTITIONED BY (`dt` STRING)
row format delimited fields terminated by '\001'
;

DROP TABLE IF EXISTS ods.ods_order_info_inc;
CREATE TABLE if not exists ods.ods_order_info_inc
(
    `id` bigint COMMENT '编号',
    `consignee` string COMMENT '收货人',
    `consignee_tel` string COMMENT '收件人电话',
    `total_amount` double COMMENT '总金额',
    `order_status` string COMMENT '订单状态',
    `user_id` bigint COMMENT '用户id',
    `payment_way` string COMMENT '付款方式',
    `delivery_address` string COMMENT '送货地址',
    `order_comment` string COMMENT '订单备注',
    `out_trade_no` string COMMENT '订单交易编号（第三方支付用)',
    `trade_body` string COMMENT '订单描述(第三方支付用)',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `expire_time` string COMMENT '失效时间',
    `process_status` string COMMENT '进度状态',
    `tracking_no` string COMMENT '物流单编号',
    `parent_order_id` string COMMENT '父订单编号',
    `img_url` string COMMENT '图片链接',
    `province_id` bigint COMMENT '省份id',
    `activity_reduce_amount` double COMMENT '活动减免金额',
    `coupon_reduce_amount` double COMMENT '优惠券减免金额',
    `original_total_amount` double COMMENT '原始总金额',
    `feight_fee` double COMMENT '运费金额',
    `feight_fee_reduce` double COMMENT '运费减免金额',
    `refundable_time` string COMMENT '可退款时间（签收后30天）'
) COMMENT '订单表'
PARTITIONED BY (`dt` STRING)
row format delimited fields terminated by '\001'