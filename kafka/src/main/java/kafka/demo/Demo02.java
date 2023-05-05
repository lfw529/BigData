package kafka.demo;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.roaringbitmap.RoaringBitmap;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 需求1: 写一个消费者，不断地从 kafka 中取消费如上 “用户行为事件” 数据，并做统计计算：
 * 每 5分钟，输出一次截止到当时的数据中出现过的用户总数
 * <p>
 * bitmap 实现
 */
public class Demo02 {
    public static void main(String[] args) {
        //用一个bitmap来记录去重guid
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();

        //启动数据消费线程
        new Thread(new ConsumeRunnableBitmap(bitmap)).start();

        // 启动一个统计及输出结果的线程(每5秒输出一次结果）
        // 优雅一点来实现定时调度，可以用各种定时调度器（有第三方的，也可以用jdk自己的：Timer）
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new StatisticBitmapTask(bitmap), 5000, 10000);
    }
}

/**
 * 消费拉取数据的线程runnable
 */
class ConsumeRunnableBitmap implements Runnable {

    RoaringBitmap bitmap;

    public ConsumeRunnableBitmap(RoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "event-01");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("lfw-events"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord<String, String> record : records) {
                String eventJson = record.value();
                // 解析json, 拿到 guid
                try {
                    UserEvent userEvent = JSON.parseObject(eventJson, UserEvent.class);

                    // 向bitmap中添加元素
                    bitmap.add((int) userEvent.getGuid());


                } catch (Exception e) {
                    System.out.println("出异常了: " + eventJson);
                }
            }
        }
    }
}

class StatisticBitmapTask extends TimerTask {

    RoaringBitmap bitmap;

    public StatisticBitmapTask(RoaringBitmap bitmap) {
        this.bitmap = bitmap;
    }

    @Override
    public void run() {
        System.out.println(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss") + ", 截止到当前的用户总数为: " + bitmap.getCardinality());
    }
}