package com.lfw.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;

public class MyUDAF extends AbstractGenericUDAFResolver {
    static final Log log = LogFactory.getLog(MyUDAF.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        return getEvaluator(info.getParameters());
    }

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        // 在这做类型检查以及选择指定的 Evaluator
        return new AverageUDAFEvaluator();
    }

    public static class AverageUDAFEvaluator extends GenericUDAFEvaluator {
        // 在这实现 UDAF 逻辑
        // Iterate 输入
        private PrimitiveObjectInspector inputOI;
        // Merge 输入
        private StructObjectInspector structOI;
        private LongObjectInspector countFieldOI;
        private DoubleObjectInspector sumFieldOI;
        private StructField countField;
        private StructField sumField;

        // TerminatePartial 输出
        private Object[] partialResult;
        // Terminate 输出
        private DoubleWritable result;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(mode, parameters);
            //初始化输入参数
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                //原始数据
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                //部分聚合数据
                structOI = (StructObjectInspector) parameters[0];
                countField = structOI.getStructFieldRef("count");
                sumField = structOI.getStructFieldRef("sum");
                countFieldOI = (LongObjectInspector) countField.getFieldObjectInspector();
                sumFieldOI = (DoubleObjectInspector) sumField.getFieldObjectInspector();
            }
            //初始化输出
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                //最终结果
                partialResult = new Object[2];
                partialResult[0] = new LongWritable(0);
                partialResult[1] = new DoubleWritable(1);
                //部分聚合结果
                //字段类型
                ArrayList<ObjectInspector> structFieldOIs = new ArrayList<ObjectInspector>();
                structFieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                structFieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                //字段名称
                ArrayList<String> structFieldNames = new ArrayList<String>();
                structFieldNames.add("count");
                structFieldNames.add("sum");
                return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldOIs);
            } else {
                result = new DoubleWritable(0);
                return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
        }

        //存储临时聚合结果对象
        static class AverageAggBuffer implements AggregationBuffer {
            long count;
            double sum;
        }

        //返回一个新的聚合对象
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AverageAggBuffer buffer = new AverageAggBuffer();
            reset(buffer);
            return buffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            AverageAggBuffer buffer = (AverageAggBuffer) agg;
            buffer.count = 0L;
            buffer.sum = 0.0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            try {
                if (parameters[0] != null) {
                    AverageAggBuffer buffer = (AverageAggBuffer) agg;
                    buffer.count++;
                    buffer.sum += PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputOI);
                }
            } catch (NumberFormatException e) {
                throw new HiveException("iterate excetion", e);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            AverageAggBuffer buffer = (AverageAggBuffer) agg;
            ((LongWritable) partialResult[0]).set(buffer.count);
            ((DoubleWritable) partialResult[1]).set(buffer.sum);
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }
            AverageAggBuffer buffer = (AverageAggBuffer) agg;
            Object partialCount = structOI.getStructFieldData(partial, countField);
            Object partialSum = structOI.getStructFieldData(partial, sumField);
            buffer.count += countFieldOI.get(partialCount);
            buffer.sum += sumFieldOI.get(partialSum);
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            AverageAggBuffer buffer = (AverageAggBuffer) agg;
            if (buffer.count == 0) {
                return null;
            }
            result.set(buffer.sum / buffer.count);
            return result;
        }
    }
}
