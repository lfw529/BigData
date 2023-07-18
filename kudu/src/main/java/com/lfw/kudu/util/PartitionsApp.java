package com.lfw.kudu.util;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;

public class PartitionsApp {
    private KuduClient kuduClient;

    private String kuduMaster;

    private String tableName;

    @Before
    public void init() {
        //初始化操作
        kuduMaster = "10.30.250.11:7051";
        //指定表名
        tableName = "rods.student";
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultAdminOperationTimeoutMs(10000);
        kuduClient = kuduClientBuilder.build();
    }

    @Test
    public void rangePartition() throws KuduException {
        //设置表的schema
        LinkedList<ColumnSchema> columnSchemas = new LinkedList<ColumnSchema>();
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CompanyId", Type.INT32).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WorkId", Type.INT32).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("Name", Type.STRING).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("Gender", Type.STRING).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("Photo", Type.STRING).key(false).build());

        //创建schema
        Schema schema = new Schema(columnSchemas);

        //创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
        //设置副本数
        tableOptions.setNumReplicas(1);

        //设置范围分区的规则
        LinkedList<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
        //设置按照那个字段进行range分区
        tableOptions.setRangePartitionColumns(parcols);

        /**
         * range
         *  0 < value < 10
         * 10 <= value < 20
         * 20 <= value < 30
         * ........
         * 80 <= value < 90
         * */
        int count = 0;
        for (int i = 0; i < 10; i++) {
            //范围开始
            PartialRow lower = schema.newPartialRow();
            lower.addInt("CompanyId", count);

            //范围结束
            PartialRow upper = schema.newPartialRow();
            count += 10;
            upper.addInt("CompanyId", count);

            //设置每一个分区的范围
            tableOptions.addRangePartition(lower, upper);
        }

        try {
            kuduClient.createTable(tableName + "_range", schema, tableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        kuduClient.close();
    }

    /**
     * 测试分区：hash分区
     */
    @Test
    public void hashPartition() throws KuduException {
        LinkedList<ColumnSchema> columnSchemas = new LinkedList<ColumnSchema>();
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CompanyId", Type.INT32).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WorkId", Type.INT32).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("Name", Type.STRING).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("Gender", Type.STRING).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("Photo", Type.STRING).key(false).build());

        //创建schema
        Schema schema = new Schema(columnSchemas);

        //创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
        //设置副本数
        tableOptions.setNumReplicas(1);
        //设置范围分区的规则
        LinkedList<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
        //设置按照那个字段进行hash分区
        tableOptions.addHashPartitions(parcols, 6);
        try {
            kuduClient.createTable(tableName + "_hash", schema, tableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }

        kuduClient.close();
    }

    /**
     * 测试分区：多级分区 Multilevel Partition
     * 混合使用hash分区和range分区
     * <p>
     * 哈希分区有利于提高写入数据的吞吐量，而范围分区可以避免tablet无限增长问题，
     * hash分区和range分区结合，可以极大的提升kudu的性能
     */

    @Test
    public void multilevelPartition() throws KuduException {
        LinkedList<ColumnSchema> columnSchemas = new LinkedList<ColumnSchema>();
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("CompanyId", Type.INT32).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("WorkId", Type.INT32).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("Name", Type.STRING).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("Gender", Type.STRING).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("Photo", Type.STRING).key(false).build());

        //创建schema
        Schema schema = new Schema(columnSchemas);

        //创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
        //设置副本数
        tableOptions.setNumReplicas(1);
        //设置范围分区的规则
        LinkedList<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
        //设置按照那个字段进行hash分区
        tableOptions.addHashPartitions(parcols, 5);

        //range分区
        int count = 0;
        for (int i = 0; i < 10; i++) {
            PartialRow lower = schema.newPartialRow();
            lower.addInt("CompanyId", count);
            count += 10;

            PartialRow upper = schema.newPartialRow();
            upper.addInt("CompanyId", count);
            tableOptions.addRangePartition(lower, upper);
        }

        try {
            kuduClient.createTable(tableName + "_multilevel", schema, tableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }

        kuduClient.close();
    }
}
