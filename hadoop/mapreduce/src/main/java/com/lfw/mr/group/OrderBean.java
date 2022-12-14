package com.lfw.mr.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private Integer orderId;  //订单id号
    private Double price;  //价格

    public OrderBean() {
        super();
    }

    public OrderBean(Integer orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override  //降序(只能用于对象间的比较)
    public int compareTo(OrderBean o) {
        int result;
        if (orderId > o.orderId) {
            result = 1;
        } else if (orderId < o.getOrderId()) {
            result = -1;
        } else {
            //价格倒序排序
            result = price > o.getPrice() ? -1 : 1;
        }
        return result;  //-1 在前面，1 在后面
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readInt();
        price = in.readDouble();
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId=" + orderId +
                ", price=" + price +
                '}';
    }
}
