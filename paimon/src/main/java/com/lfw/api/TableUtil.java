package com.lfw.api;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

public class TableUtil {

    public static void createTable() throws Exception {
        // 指定Schema（表的名称、字段的名称、类型、能不能为空等等）
        Schema.Builder schemaBuilder = new Schema.Builder();
        // 定义多个字段，指定名称和类型
        schemaBuilder.column("id", DataTypes.INT());
        schemaBuilder.column("dt", DataTypes.STRING());
        schemaBuilder.column("name", DataTypes.STRING());
        // 指定主键
        schemaBuilder.primaryKey("id");
        // 指定分区字段
        schemaBuilder.partitionKeys("dt");
        // 构建 Schema
        Schema schema = schemaBuilder.build();
        // 创建表
        Catalog catalog = CatalogUtil.createFilesystemCatalog();
//        Catalog catalog = CatalogUtil.createHiveCatalog();
        Identifier identifier = Identifier.create("lfw", "tb_users");
        catalog.createTable(identifier, schema, true);
    }

    public static Table getTable(String dbName, String tableName) throws Exception {
        // 获取 Catalog 对象
        Catalog catalog = CatalogUtil.createFilesystemCatalog();
        Identifier identifier = Identifier.create(dbName, tableName);
        return catalog.getTable(identifier);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "lfw");
        createTable();

        Table table = getTable("lfw", "tb_users");
        System.out.println(table);
    }
}
