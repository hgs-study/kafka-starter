import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class SimpleProducer {
    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(PropertiesFactory.getProperties());

        String messageValue = "testMessage";
        // 토픽명, 메세지
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,  messageValue);

        // 토픽명, 메세지, 키 지정
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Pangyo",  "23");

        //토픽명, 메세지, 키 지정, 파티션 번호
        int partitionNo = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, "Pangyo",  "23");

        producer.send(record);
        log.info("{}", record);
        producer.flush();
        producer.close();
    }
}
