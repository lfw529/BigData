package kafka.adminclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminClientDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        //key, value 序列化(必须)：key.serializer, value.serializer
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        // 管理客户端
        AdminClient adminClient = KafkaAdminClient.create(props);

        // 创建一个topic：这个创建代码只需第一次执行一次
//        NewTopic clientTest = new NewTopic("client-test", 3, (short) 2);
//        adminClient.createTopics(Arrays.asList(clientTest));

        // 查看一个topic的详细信息, 给一个 topic名称
        DescribeTopicsResult topicDescriptions = adminClient.describeTopics(Arrays.asList("client-test"));

        KafkaFuture<Map<String, TopicDescription>> descriptions = topicDescriptions.all();
        Map<String, TopicDescription> infos = descriptions.get();
        Set<Map.Entry<String, TopicDescription>> entries = infos.entrySet();
        for (Map.Entry<String, TopicDescription> entry : entries) {
            String topicName = entry.getKey();
            TopicDescription td = entry.getValue();
            List<TopicPartitionInfo> partitions = td.partitions();
            for (TopicPartitionInfo partition : partitions) {
                int partitionIndex = partition.partition();
                List<Node> replicas = partition.replicas();
                List<Node> isr = partition.isr();
                Node leader = partition.leader();
                System.out.println(topicName + "\t" + partitionIndex + "\t" + replicas + "\t" + isr + "\t" + leader);
/*              client-test	0	[hadoop103:9092 (id: 1 rack: null), hadoop104:9092 (id: 2 rack: null)]	[hadoop103:9092 (id: 1 rack: null), hadoop104:9092 (id: 2 rack: null)]	hadoop103:9092 (id: 1 rack: null)
                client-test	1	[hadoop102:9092 (id: 0 rack: null), hadoop103:9092 (id: 1 rack: null)]	[hadoop102:9092 (id: 0 rack: null), hadoop103:9092 (id: 1 rack: null)]	hadoop102:9092 (id: 0 rack: null)
                client-test	2	[hadoop104:9092 (id: 2 rack: null), hadoop102:9092 (id: 0 rack: null)]	[hadoop104:9092 (id: 2 rack: null), hadoop102:9092 (id: 0 rack: null)]	hadoop104:9092 (id: 2 rack: null)
*/
            }
        }

        adminClient.close();
    }
}
