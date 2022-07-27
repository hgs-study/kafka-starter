import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

/**
 * 개별 레코드 단위로 매번 오프셋 커밋
 */
@Slf4j
public class SyncOffsetCommitConsumer {
    public static void main(String[] args) {
        Properties properties = PropertiesFactory.getProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(PropertiesFactory.TOPIC_NAME));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            HashMap<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

            for (ConsumerRecord<String, String> record : records) {
                log.info("record : {} ", record);
                currentOffset.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() +1 , null));
                consumer.commitSync(currentOffset);
            }
        }
    }
}
