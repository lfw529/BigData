package com.lfw.kudu.util;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class CRUDApp {
    private KuduClient kuduClient;

    private String kuduMaster;

    private String tableName;

    @Before
    public void init() {
        //初始化操作
        kuduMaster = "hadoop102:7051";
        //指定表名
        tableName = "student";
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultAdminOperationTimeoutMs(10000);
        kuduClient = kuduClientBuilder.build();
    }

    //创建表
    @Test
    public void createTable() throws KuduException {
        //判断表是否存在，不存在就构建
        if (!kuduClient.tableExists(tableName)) {
            //构建表的schema信息  ---- 就是表的字段和类型
            ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("age", Type.INT32).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("sex", Type.INT32).build());

            Schema schema = new Schema(columnSchemas);
            //指定创建表的相关属性
            CreateTableOptions options = new CreateTableOptions();
            ArrayList<String> partitionList = new ArrayList<String>();
            //指定kudu表的分区字段是什么
            partitionList.add("id");   //按照 id.hashcode % 分区数 = 分区号
            options.addHashPartitions(partitionList, 6);
            options.setNumReplicas(1);  //设置为一个副本
            kuduClient.createTable(tableName, schema, options);
        }
    }


    // 插入数据
    @Test
    public void insertTable() throws KuduException {
        //想表加载数据需要一个KuduSession对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用KuduTable来构建Operation的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
        for (int i = 0; i <= 10; i++) {
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("id", i);
            row.addString("name", "zhangsan-" + i);
            row.addInt("age", 20 + i);
            row.addInt("sex", i % 2);
            kuduSession.apply(insert);   //最后实现执行数据的加载操作
        }
    }

    //查询数据
    @Test
    public void queryData() throws KuduException {
        //构建一个查询的扫描器
        KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient.newScannerBuilder(kuduClient.openTable(tableName));
        ArrayList<String> columnsList = new ArrayList<String>();
        columnsList.add("id");
        columnsList.add("name");
        columnsList.add("age");
        columnsList.add("sex");
        //提交需要扫描的列
        kuduScannerBuilder.setProjectedColumnNames(columnsList);
        //返回结果集
        KuduScanner kuduScanner = kuduScannerBuilder.build();
        //遍历
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult row = rowResults.next();
                int id = row.getInt("id");
                String name = row.getString("name");
                int age = row.getInt("age");
                int sex = row.getInt("sex");
                System.out.println("id=" + id + "  name=" + name + "  age=" + age + "  sex=" + sex);
            }
        }
    }

    //更新数据
    @Test
    public void updateData() throws KuduException {
        //修改表的数据需要一个KuduSession对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        // 需要使用kuduTable来构建Operation的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);

        Upsert upsert = kuduTable.newUpsert();  //如果id存在就表示修改，不存在就新增
        PartialRow row = upsert.getRow();
        row.addInt("id", 9);
        row.addString("name", "zhangsan-100");
        row.addInt("age", 100);
        row.addInt("sex", 0);

        kuduSession.apply(upsert);// 最后实现执行数据的修改操作
    }

    //删除一条数据
    @Test
    public void deleteData() throws KuduException {
        // 删除表的数据需要一个kuduSession对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        // 需要使用kuduTable来构建Operation的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);

        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();
        row.addInt("id", 9);

        kuduSession.apply(delete);// 最后实现执行数据的删除操作
    }

    //删除表
    @Test
    public void dropTable() throws KuduException {
        if (kuduClient.tableExists(tableName)) {
            kuduClient.deleteTable(tableName);
        }
    }
}
