package kafka.demo;

import com.alibaba.fastjson.JSON;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 需求2: 写一个消费者，不断地从 kafka 中取消费如上 “用户行为事件” 数据，并做如下加工处理：
 * 给每一条数据，添加一个字段，来标识，该条数据所属的用户的id在今天是否是第一次出现，如是，则标注 1；否则，标注0
 * {"guid":1,"eventId":"pageview","timeStamp":1637868346789,"flag":1}
 * {"guid":1,"eventId":"addcart","timeStamp":1637868346966,"flag":0}
 * {"guid":2,"eventId":"applaunch","timeStamp":1637868346967,"flag":1}
 */
public class Demo03 {
    public static void main(String[] args) {
        // 启动数据消费线程
        new Thread(new ConsumeRunnableBloomFilter()).start();
    }

    /**
     * 消费拉取数据的线程runnable
     */
    static class ConsumeRunnableBloomFilter implements Runnable {

        BloomFilter<Long> bloomFilter;

        KafkaConsumer<String, String> consumer;

        public ConsumeRunnableBloomFilter() {
            //初始化：10亿容量，假阳率：0.01
            bloomFilter = BloomFilter.create(Funnels.longFunnel(), 1000000000, 0.01);

            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "event-01");

            consumer = new KafkaConsumer<>(props);

        }

        @Override
        public void run() {
            consumer.subscribe(Arrays.asList("lfw-events"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : records) {
                    String eventJson = record.value();
                    // 解析json, 拿到 guid
                    try {
                        UserEvent userEvent = JSON.parseObject(eventJson, UserEvent.class);

                        // 去布隆过滤器中判断一下，本次出现guid，是否曾经已经记录过
                        boolean mightContain = bloomFilter.mightContain(userEvent.getGuid());

                        // 如果本次出现的guid已存在，则将flag设置为0
                        if (mightContain) {
                            userEvent.setFlag(0);
                        }
                        // 如果本次出现的guid不存在，则将flag设置为1，并将本次出现的guid映射到布隆过滤
                        else {
                            userEvent.setFlag(1);
                            // 向布隆过滤器中映射新的元素
                            bloomFilter.put(userEvent.getGuid());
                        }

                        // 输出结果
                        System.out.println(JSON.toJSONString(userEvent));
                        //连续注入两条相同的数据验证 flag
//                        {"eventId":"YlLIq","flag":1,"guid":41,"timeStamp":1682486550313}
//                        {"eventId":"YlLIq","flag":0,"guid":41,"timeStamp":1682486550313}

                    } catch (Exception e) {
                        System.out.println("出异常了：" + eventJson);
                    }
                }
            }
        }
    }
}
