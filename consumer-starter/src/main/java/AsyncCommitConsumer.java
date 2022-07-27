import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class AsyncCommitConsumer {
    public static void main(String[] args) {
        Properties properties = PropertiesFactory.getProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(PropertiesFactory.TOPIC_NAME));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("record : {}", record);
            }

            // commitAsync()의 응답을 받을 수 있도록 도와주는 콜백 인터페이
            consumer.commitAsync(new OffsetCommitCallback() {
                // offsets : 커밋 완료된 오프셋 정보
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if(exception != null){
                        System.err.println("Commit failed");
                    }else{
                        System.out.println("Commit succeeded");
                    }
                    if(exception != null){
                        log.error("Commit failed for offsets : {}", offsets, exception);
                    }
                }
            });
        }
    }
}
