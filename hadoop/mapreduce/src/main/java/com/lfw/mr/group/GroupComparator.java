package com.lfw.mr.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 排序/分组比较器需要继承hadoop提供的 WritableComparator类
 */
public class GroupComparator extends WritableComparator {
    public GroupComparator() {
        super(OrderBean.class, true);
    }

    /**
     * 分组比较规则: 只比orderId
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        return aBean.getOrderId().compareTo(bBean.getOrderId());
    }
}
