package com.lfw.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

@Description(name = "count", value = "_FUNC_(*) - Returns the total number of retrieved rows, including rows containing NULL values.\n_FUNC_(expr) - Returns the number of rows for which the supplied expression is non-NULL.\n_FUNC_(DISTINCT expr[, expr...]) - Returns the number of rows for which the supplied expression(s) are unique and non-NULL.")
public class MyUDAF implements GenericUDAFResolver2 {
    private static final Log LOG;

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return new GenericUDAFCountEvaluator();
    }

    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo) throws SemanticException {
        TypeInfo[] parameters = paramInfo.getParameters();

        if (parameters.length == 0) {
            if (!(paramInfo.isAllColumns())) {
                throw new UDFArgumentException("Argument expected");
            }
        } else {
            if ((parameters.length > 1) && (!(paramInfo.isDistinct()))) {
                throw new UDFArgumentException("DISTINCT keyword must be specified");
            }
            assert (!(paramInfo.isAllColumns())) : "* not supported in expression list";
        }

        return new GenericUDAFCountEvaluator().setCountAllColumns(paramInfo.isAllColumns());
    }

    static {
        LOG = LogFactory.getLog(MyUDAF.class.getName());
    }

    public static class GenericUDAFCountEvaluator extends GenericUDAFEvaluator {
        private boolean countAllColumns;
        private LongObjectInspector partialCountAggOI;
        private LongWritable result;

        public GenericUDAFCountEvaluator() {
            this.countAllColumns = false;
        }

        public ObjectInspector init(GenericUDAFEvaluator.Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            this.partialCountAggOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;

            this.result = new LongWritable(0L);
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        private GenericUDAFCountEvaluator setCountAllColumns(boolean countAllCols) {
            this.countAllColumns = countAllCols;
            return this;
        }

        public GenericUDAFEvaluator.AggregationBuffer getNewAggregationBuffer() throws HiveException {
            CountAgg buffer = new CountAgg();
            reset(buffer);
            return buffer;
        }

        public void reset(GenericUDAFEvaluator.AggregationBuffer agg) throws HiveException {
            ((CountAgg) agg).value = 0L;
        }

        public void iterate(GenericUDAFEvaluator.AggregationBuffer agg, Object[] parameters) throws HiveException {
            if (parameters == null) {
                return;
            }
            if (this.countAllColumns) {
                assert (parameters.length == 0);
                ((CountAgg) agg).value += 1L;
            } else {
                assert (parameters.length > 0);
                boolean countThisRow = true;
                for (Object nextParam : parameters) {
                    if (nextParam == null) {
                        countThisRow = false;
                        break;
                    }
                }
                if (countThisRow)
                    ((CountAgg) agg).value += 1L;
            }
        }

        public void merge(GenericUDAFEvaluator.AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                long p = this.partialCountAggOI.get(partial);
                ((CountAgg) agg).value += p;
            }
        }

        public Object terminate(GenericUDAFEvaluator.AggregationBuffer agg) throws HiveException {
            this.result.set(((CountAgg) agg).value);
            return this.result;
        }

        public Object terminatePartial(GenericUDAFEvaluator.AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @GenericUDAFEvaluator.AggregationType(estimable = true)
        static class CountAgg extends GenericUDAFEvaluator.AbstractAggregationBuffer {
            long value;

            public int estimate() {
                return 8;
            }
        }
    }
}