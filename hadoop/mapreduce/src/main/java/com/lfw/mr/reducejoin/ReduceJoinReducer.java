package com.lfw.mr.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinReducer extends Reducer<Text, OrderPdBean, OrderPdBean, NullWritable> {

    //用于存储每组中的所有的order数据
    private final List<OrderPdBean> orders = new ArrayList<>();

    //用于描述每组中的pd数据
    private final OrderPdBean pd = new OrderPdBean();

    /**
     * hadoop中 k 和 v 的真相:  每次进行迭代处理时，k和v对应的对象一直是同一个。每次会修改对象中属性的值.
     */
    @Override
    protected void reduce(Text key, Iterable<OrderPdBean> values, Context context) throws IOException, InterruptedException {
        for (OrderPdBean orderPdBean : values) {
            if ("order".equals(orderPdBean.getTitle())) {
                //来自于order的数据
                try {
                    OrderPdBean currentOrderPdBean = new OrderPdBean();
                    BeanUtils.copyProperties(currentOrderPdBean, orderPdBean);
                    orders.add(currentOrderPdBean);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                //来自于pd的数据
                try {
                    BeanUtils.copyProperties(pd, orderPdBean);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        // 从orders中迭代每个order对象，与pd对象进行join
        for (OrderPdBean order : orders) {
            order.setPname(pd.getPname());
            //写出
            context.write(order, NullWritable.get());
        }
        //清空集合
        orders.clear();
    }
}
