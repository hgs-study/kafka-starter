import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;

/**
 * 문제 : poll()로 데이터를 반환받고 커밋하지 않은채 장애가 생기면 리밸런싱이 진행되는데
 *       이때, 중복 데이터가 생길 수 있음밋 (오프셋은 아직 이전 오프셋이기 때문)
 * 해결 : 리밸런싱 전에 커밋 (ConsumerRebalanceListener 구현체 onPartitionsRevoked에서 컨슈머 커밋)
 */
@Slf4j
public class RebalanceListenerConsumer {

    private static final HashMap<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        Properties properties = PropertiesFactory.getProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(PropertiesFactory.TOPIC_NAME), new RebalanceListener(consumer));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record {} : ",record);

                // offset+1 : 컨슈머 재시작 시 파티션에서 가장 마지막으로 커밋된 오프셋부터 레코드를 읽기 때문
                currentOffset.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1 , null));
                consumer.commitSync(currentOffset);
            }
        }
    }

    private static class RebalanceListener implements ConsumerRebalanceListener{

        private final KafkaConsumer<String, String> consumer;

        public RebalanceListener(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        /**
         * 리밸런스가 시작되기 직전에 호출되는 메서드
         * 마지막으로 처리한 레코드를 기준으로 커밋을 하기 위해서는
         * 리밸런스가 시작하기 직전에 커밋하면 되므로 해당 메서드에 커밋을 구현하면된다.
         * @param partitions
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.warn("Partitions are revoked");
            consumer.commitSync(currentOffset);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.warn("Partitions are assigned");
        }
    }
}
