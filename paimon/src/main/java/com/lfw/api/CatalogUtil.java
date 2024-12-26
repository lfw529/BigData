package com.lfw.api;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

public class CatalogUtil {
    /**
     * 创建文件系统的 Catalog
     *
     * @return
     */
    public static Catalog createFilesystemCatalog() {
        // 创建用于保存KV的Options
        Options options = new Options();
        options.set("type", "paimon");
        options.set("warehouse", "hdfs://192.168.10.101:8020/paimon");
        // 将Options传入到CatalogContext的create方法，创建CatalogContext
        CatalogContext context = CatalogContext.create(options);
        // 使用CatalogFactory，调用createCatalog，将CatalogContext，创建Catalog
        return CatalogFactory.createCatalog(context);
    }

    /**
     * 创建HiveCatalog
     *
     * @return
     */
    public static Catalog createHiveCatalog() {
        //创建用于保存KV的Options
        Options options = new Options();
        options.set("type", "paimon");
        options.set("metastore", "hive"); //指定metastore为hive
        //指定paimon创建的DB、Table保存的位置
        options.set("warehouse", "hdfs://192.168.10.101:8020/user/paimon/warehouse");
        //指定Hive的MateStore服务的地址
        options.set("uri", "thrift://192.168.10.101:9083");
        //指定Hive的配置文件
        options.set("hive-conf-dir", "/Users/star/IdeaProjects/paimon-in-action/src/main/resources");
        //options.set("hadoop-conf-dir", "...");
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        //Catalog catalog = createFilesystemCatalog();
        Catalog catalog = createHiveCatalog();
        //创建Database
        catalog.createDatabase("lfw", true);
    }
}
