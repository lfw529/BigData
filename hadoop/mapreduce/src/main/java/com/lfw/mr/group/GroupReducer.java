package com.lfw.mr.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<OrderBean, Text, OrderBean, Text> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //因为分组比较器决定 orderId 相同的数据进入到同一个 reduce 方法，且我们的排序规则决定进来的数据是价格降序的
        //因此只需要取 reduce 中的第一条数据就是最高的价格。
        context.write(key, values.iterator().next());
    }
}
