package com.lfw.kudu;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;

public class KuduTableSource extends InputFormatTableSource {
    @Override
    public InputFormat getInputFormat() {
        return new KuduInputFormat();
    }

    @Override
    public TableSchema getTableSchema() {
        String[] names = {"info"};
        DataType[] dataTypes = {DataTypes.STRING()};
        TableSchema tableSchema = TableSchema.builder().fields(names, dataTypes).build();
        return tableSchema;
    }

    @Override
    public DataType getProducedDataType() {
        return getTableSchema().getFieldDataTypes()[0];
    }
}
