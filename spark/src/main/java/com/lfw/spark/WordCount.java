package com.lfw.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        //创建JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //使用 JavaSparkContext 创建RDD
        JavaRDD<String> lines = jsc.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //将单词组聚合
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return Tuple2.apply(word, 1);
                    }
                }
        );
        //分组聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
        //排序，先调换KV的顺序VK
        JavaPairRDD<Integer, String> swapped = reduced.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) throws Exception {
                        return tp.swap();
                    }
                }
        );
        //再排序
        JavaPairRDD<Integer, String> sorted = swapped.sortByKey(false);
        //再调换顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(
                new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) throws Exception {
                        return tp.swap();
                    }

                });
        //触发Action，将数据保存到HDFS
        result.saveAsTextFile(args[1]);
        //释放资源
        jsc.stop();
    }
}
