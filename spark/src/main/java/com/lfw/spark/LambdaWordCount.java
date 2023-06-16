package com.lfw.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class LambdaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaLambdaWordCount");
        //创建SparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //创建RDD
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将单词和一组合
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> Tuple2.apply(word, 1));
        //分组聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((a, b) -> a + b);
        //调换顺序
        JavaPairRDD<Integer, String> swapped = reduced.mapToPair(tp -> tp.swap());
        //排序
        JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);
        //调换顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());
        //将数据保存到HDFS
        result.saveAsTextFile(args[1]);
        //释放资源
        jsc.stop();
    }
}
