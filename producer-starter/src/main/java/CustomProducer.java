import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class CustomProducer {
    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        // applied custom partitioner
        Properties properties = PropertiesFactory.getProperties();
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //토픽명, 메세지, 키 지정, 파티션 번호
        int partitionNo = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, "Pangyo",  "23");

        producer.send(record);
        log.info("{}", record);
        producer.flush();
        producer.close();
    }
}
