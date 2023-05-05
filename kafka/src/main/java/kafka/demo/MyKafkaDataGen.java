package kafka.demo;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * 创建一个 topic:
 * # kafka-topics.sh --create --topic lfw-events --partitions 3 --replication-factor 2 --bootstrap-server hadoop102:9092
 * <p>
 * 可以用命令去监视这个 topic 是否有数据到达：
 * # kafka-console-consumer.sh --topic lfw-events --bootstrap-server hadoop102:9092
 * <p>
 * 需求：
 * 写一个生产者，不断去生成 “用户行为事件” 数据，并写入 kafka
 * {"guid":1,"eventId":"pageview","timeStamp":1637868346789}
 * {"guid":1,"eventId":"addcart","timeStamp":1637868346966}
 * {"guid":2,"eventId":"applaunch","timeStamp":1637868346967}
 * .....
 * <p>
 * 需求1: 写一个消费者，不断地从 kafka 中取消费如上 “用户行为事件” 数据，并做统计计算：
 * 每 5分钟，输出一次截止到当时的数据中出现过的用户总数
 * <p>
 * 需求2: 写一个消费者，不断地从 kafka 中取消费如上 “用户行为事件” 数据，并做如下加工处理：
 * 给每一条数据，添加一个字段，来标识，该条数据所属的用户的id在今天是否是第一次出现，如是，则标注1 ；否则，标注0
 * {"guid":1,"eventId":"pageview","timeStamp":1637868346789,"flag":1}
 * {"guid":1,"eventId":"addcart","timeStamp":1637868346966,"flag":0}
 * {"guid":2,"eventId":"applaunch","timeStamp":1637868346967,"flag":1}
 * .......
 * <p>
 * 需求3: 写一个消费者，不断地从 kafka 中取消费如上 “用户行为事件” 数据，并做统计计算：
 * 每5分钟，统计最近10分钟内的用户总数并输出
 */
public class MyKafkaDataGen {

    /**
     * 业务数据生成器
     *
     * @param args
     */
    KafkaProducer<String, String> producer;

    public MyKafkaDataGen() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<String, String>(props);
    }

    public void genData() throws InterruptedException {
        UserEvent userEvent = new UserEvent();
        while (true) {
            //造一条随机的用户行为事件数据对象
            userEvent.setGuid(RandomUtils.nextInt(1, 10000));
            userEvent.setEventId(RandomStringUtils.randomAlphabetic(5, 8));
            userEvent.setTimeStamp(System.currentTimeMillis());

            //转成json串
            String json = JSON.toJSONString(userEvent);

            //将业务数据封装成ProducerRecord对象
            ProducerRecord<String, String> record = new ProducerRecord<>("lfw-events", json);

            //用producer写入kafka
            producer.send(record);

            //控制发送的速度
            Thread.sleep(RandomUtils.nextInt(200, 1500));
        }
    }

    public static void main(String[] args) throws InterruptedException {
        MyKafkaDataGen myKafkaDataGen = new MyKafkaDataGen();
        myKafkaDataGen.genData();
    }
}

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
class UserEvent {
    private long guid;
    private String eventId;
    private long timeStamp;
    private Integer flag;
}