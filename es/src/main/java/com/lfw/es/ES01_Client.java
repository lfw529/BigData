package com.lfw.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;


public class ES01_Client {

    public static void main(String[] args) throws IOException {
        // 创建客户端对象
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("bigdata01", 9200, "http"))
        );

        // 关闭ES客户端
        esClient.close();
    }
}
