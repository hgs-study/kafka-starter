import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class AsyncCallbackProducer {
    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        Properties properties = PropertiesFactory.getProperties();
        String messageValue = "testMessage";

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);

        producer.send(record, new ProducerCallback());
    }
}
