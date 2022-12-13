package com.lfw.mr.writable_partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器 继承Partitioner类，并重写getPartition方法
 * <p>
 * Partitioner<KEY,VALUE> :
 * KEY: 写Mapper输出的key的类型
 * VALUE: 写Mapper输出的value的类型
 */
public class PhoneNumPartitioner extends Partitioner<Text, FlowBean> {

    /**
     * 分区的规则: 按照手机号的前三位进行分区
     * 136  --> 0
     * 137  --> 1
     * 138  --> 2
     * 139  --> 3
     * 其他  --> 4
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //获取手机号
        String phoneNum = text.toString();
        //计算分区
        int partition;
        if (phoneNum.startsWith("136")) {
            partition = 0;
        } else if (phoneNum.startsWith("137")) {
            partition = 1;
        } else if (phoneNum.startsWith("138")) {
            partition = 2;
        } else if (phoneNum.startsWith("139")) {
            partition = 3;
        } else {
            partition = 4;
        }
        //最后返回分区好号
        return partition;
    }
}

