package com.lfw.kudu;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class KuduInputFormat extends GenericInputFormat<String> { private static Logger LOG = LoggerFactory.getLogger(KuduInputFormat.class);

    private KuduClient client;
    private KuduTable table;
    private KuduScanner scanner;
    private static final String IMPALA_TABLE_PREFIX = "impala::";
    private static final String KUDU_MASTER_ADDRESS = "192.168.1.1:7051";
    private String tableName = "class";
    List<String> columnsList = new ArrayList<>();
    private RowResultIterator iterator;

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);
        client = new KuduClient.KuduClientBuilder(KUDU_MASTER_ADDRESS).build();
        table = client.openTable(IMPALA_TABLE_PREFIX + tableName);
        columnsList.add("id");
        columnsList.add("teacher_id");
        scanner = client.newScannerBuilder(table).setProjectedColumnNames(columnsList).build();
        iterator = scanner.nextRows();
    }

    @Override
    public boolean reachedEnd() {
        return iterator.hasNext() == false;
    }

    @Override
    public String nextRecord(String reuse) {
        StringJoiner joiner = new StringJoiner(":");
        RowResult rowResult = iterator.next();
        String id = String.valueOf(rowResult.getLong("id"));
        String teacher_id = String.valueOf(rowResult.getLong("teacher_id"));
        joiner.add(id).add(teacher_id);
        return joiner.toString();
    }

    @Override
    public GenericInputSplit[] createInputSplits(int numSplits) {
        GenericInputSplit[] splits = new GenericInputSplit[1];
        splits[0] = new GenericInputSplit(0, 1);
        return splits;
    }

    @Override
    public void close() throws IOException {
        scanner.close();
        client.close();
    }
}
