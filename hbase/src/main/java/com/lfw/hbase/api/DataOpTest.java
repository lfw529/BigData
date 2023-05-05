package com.lfw.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 增删改查
 */
public class DataOpTest {
    // Connection是一个重量级的对象，不能频繁去创建Connection
    // Connection是线程安全的
    private Connection conn;
    private Table table;

    @BeforeTest
    public void beforeTest() throws IOException {
        // 1.使用HbaseConfiguration.create()创建Hbase配置
        Configuration conf = HBaseConfiguration.create();
        // 2.使用ConnectionFactory.createConnection()创建Hbase连接
        conn = ConnectionFactory.createConnection(conf);

        table = conn.getTable(TableName.valueOf("lfw_01:students"));
    }

    /**
     * 插入数据
     * <p>
     * 测试前先创建一个表：
     * create 'lfw_01:students', 'base_info', 'extra_info'
     */
    @Test
    public void putTest() throws IOException {
        //读数据源
        BufferedReader br = new BufferedReader(new FileReader("data/students.txt"));
        String line;

        ArrayList<Put> puts = new ArrayList<>();

        long start = System.currentTimeMillis();
        while ((line = br.readLine()) != null) {
            String[] split = line.split(",");

            // 000001,zhangsan,18,male,beijing,18000,2018-07-01
            String rkMD5 = MD5Hash.getMD5AsHex(split[0].getBytes());  //对 rowkey 进行MD5加密

            //构造一个用于封装KeyValue数据的put对象
            Put put = new Put(Bytes.toBytes(rkMD5));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(Integer.parseInt(split[2])));
            put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("gender"), Bytes.toBytes(split[3]));
            put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("city"), Bytes.toBytes(split[4]));
            put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("salary"), Bytes.toBytes(Integer.parseInt(split[5])));
            put.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("graduate"), Bytes.toBytes(split[6]));

            // 把封装好的一行数据，先暂存在一个list中
            puts.add(put);

            // 判断list中的条数是否满足批次大小（100条一批）
            if (puts.size() >= 100) {
                // 插入数据
                table.put(puts);
                // 清空list
                puts.clear();
            }
        }

        //最后一批
        if (puts.size() > 0) {
            table.put(puts);
        }

        long end = System.currentTimeMillis();

        // 关闭连接
        table.close();
        conn.close();

        System.out.println("总耗时： " + (end - start));
    }

    /**
     * 取一个指定的 KeyValue
     */
    @Test
    public void getKeyValueTest() throws IOException {
        //MD5 解码
        String rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes("000190"));

        //使用 rowkey 构建 get 对象
        Get getParam = new Get(Bytes.toBytes(rowKey));
        //选择要获取的列，封装到 getParam 中
        getParam.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
        //提取 key:value
        Result result = table.get(getParam);
        byte[] value = result.getValue(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
        int age = Bytes.toInt(value);
        System.out.println(age);

        table.close();

    }

    /**
     * get 单行中的指定列族的所有KeyValue数据
     */
    @Test
    public void getFamilyTest() throws IOException {
        String rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes("000010"));

        Get getParam = new Get(Bytes.toBytes(rowKey));
        getParam.addFamily(Bytes.toBytes("base_info"));

        Result result = table.get(getParam);
        //构建一个单元格扫描器
        CellScanner cellScanner = result.cellScanner();
        //advance:每次向前移动一个单元格
        while (cellScanner.advance()) {
            //一个 cell 就代表一个 KeyValue
            Cell cell = cellScanner.current();

            //rowkey获取
            byte[] rowKeyBytes = CellUtil.cloneRow(cell);
            //列族获取
            byte[] familyBytes = CellUtil.cloneFamily(cell);
            //列 key 获取
            byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
            //列 value 获取
            byte[] valueBytes = CellUtil.cloneValue(cell);

            String qualifier = Bytes.toString(qualifierBytes);
            String value = "";
            // 只有年龄需要转换成 string
            if ("age".equals(qualifier)) {
                value = Bytes.toInt(valueBytes) + "";
            } else {
                value = Bytes.toString(valueBytes);
            }

            //打印输出
            System.out.println(Bytes.toString(rowKeyBytes) + ","
                    + Bytes.toString(familyBytes) + ","
                    + qualifier + ","
                    + value
            );

            table.close();
        }
    }

    /**
     * 扫描指定行范围的数据
     */
    @Test
    public void scanRowsTest() throws IOException {
        //构建一个 扫描器 对象，对应scan指令
        Scan scan = new Scan();
        //MD5解码
        String startMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes("000190"));
        String stopMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes("000200"));
        //设置了扫描的起始行和结束行（默认含首不含尾）
        scan.withStartRow(Bytes.toBytes(startMd5));
        //inclusive: 扫描时是否包含停止行，即最后一行。
//        scan.withStopRow(Bytes.toBytes(stopMd5), false);

        // 设置总共要扫描的行数
        scan.setLimit(10);

        ResultScanner scanner = table.getScanner(scan);
        // 扫描时的行迭代器
        Iterator<Result> iterator = scanner.iterator();
        printData(iterator);

        table.close();
    }

    /**
     * 用于打印 scan 结果的工具方法
     */
    private static void printData(Iterator<Result> iterator) throws IOException {
        while (iterator.hasNext()) {  // 行迭代
            // 迭代到一行
            Result result = iterator.next();
            // 拿到行中的cell单元格的迭代器
            CellScanner cellScanner = result.cellScanner();
            // 用单元格迭代器，迭代行中的每一个 cell(单元格，KeyValue)
            while (cellScanner.advance()) {   //  行内的keyValue迭代
                Cell cell = cellScanner.current();

                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = "";
                if (qualifier.equals("age") || qualifier.equals("salary")) {
                    value = Bytes.toInt(CellUtil.cloneValue(cell)) + "";
                } else {
                    value = Bytes.toString(CellUtil.cloneValue(cell));
                }

                System.out.println(rowKey + "," + family + "," + qualifier + "," + value);
            }
        }
    }

    /**
     * 扫描多行数据，并带过滤条件
     */
    @Test
    public void scanRowsWithFilterTest() throws IOException {
        Scan scan = new Scan();
        //从这个 rowkey 开始扫描十行
        scan.withStartRow(Bytes.toBytes("eccb22fe1acf4027649239c7d6383d78"), true);
        scan.setLimit(10);

        // 行键前缀过滤器：选择 ecc 开头的行键
        Filter filter1 = new PrefixFilter(Bytes.toBytes("ecc"));  // 行键前缀过滤器

        // 列值过滤器（匹配到条件的行，整行数据都将返回）
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("extra_info"), Bytes.toBytes("city"), CompareOperator.EQUAL, Bytes.toBytes("南京"));

        ArrayList<Filter> filterList = new ArrayList<>();
        filterList.add(filter1);
        filterList.add(filter2);

        // 将上面的2个过滤器，组成一个 MUST_PASS_ALL 关系的过滤器组
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
        //FilterListWithAND filters = new FilterListWithAND(filterList);

        // 给scan参数设置过滤器条件
        scan.setFilter(filters);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();  // 拿到行迭代器

        printData(iterator);  // 迭代，并打印数据

    }


    @AfterTest
    public void afterTest() throws IOException {
        conn.close();
    }
}