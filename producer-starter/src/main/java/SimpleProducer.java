import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class SimpleProducer {
    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(PropertiesFactory.getProperties());

        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        producer.send(record);
        log.info("{}", record);
        producer.flush();
        producer.close();
    }
}
