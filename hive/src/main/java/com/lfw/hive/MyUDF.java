package com.lfw.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义 UDF 函数，需要继承 GenericUDF 类
 * 需求：计算指定字符串的长度
 */
public class MyUDF extends GenericUDF {
    /**
     * @return 返回值类型鉴别器对象
     * @throws UDFArgumentException
     * @Param arguments 输入参数类型的鉴别器对象
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //判断输入参数的个数
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("参数个数不为1");
        }
        //判断输入参数的类型
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(0, "Input Args Type Error!!!");
        }
        //函数本身返回值为 int, 需要返回 int 类型的鉴别器对象
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的逻辑处理
     *
     * @param arguments 输入的参数
     * @return 返回值
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        //1.取出输入数据
        String input = arguments[0].get().toString();
        //2.判断输入数据是否为null
        if (input == null) {
            return 0;
        }
        //3.返回输入数据长度
        return input.length();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "";
    }
}
