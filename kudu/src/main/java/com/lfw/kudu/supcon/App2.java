package com.lfw.kudu.supcon;

import com.lfw.kudu.supcon.sql.Table_cdc_test;
import com.lfw.kudu.supcon.sql.Table_movie_info;
import com.lfw.kudu.supcon.utils.HiveConnect;

public class App2 {
    static HiveConnect hiveConnect = new HiveConnect();

    public static void main(String[] args) {
//        hiveConnect.process(Table_movie_info.source());
        hiveConnect.process(Table_cdc_test.query());
    }
}
