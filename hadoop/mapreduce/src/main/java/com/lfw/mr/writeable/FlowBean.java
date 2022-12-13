package com.lfw.mr.writeable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private Integer upFlow; // 上行流量
    private Integer downFlow; // 下行流量
    private Integer sumFlow; //总流量

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Integer sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.getUpFlow() + this.getDownFlow();
    }

    public FlowBean() {
    }

    @Override   //序列化方法
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(sumFlow);
    }

    @Override  //反序列化方法
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.sumFlow = in.readInt();
    }

    @Override
    public String toString() {
        return this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
    }
}
