import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 직접 파티션을 컨슈머에 명시적으로 할당
 */
@Slf4j
public class ExactPartitionConsumer {
    public static void main(String[] args) {
        String TOPIC_NAME = "test";
        int PARTITION_NUMBER = 0;

        Properties properties = PropertiesFactory.getProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record {} : ", record);
            }
            consumer.commitSync();
        }
    }
}
