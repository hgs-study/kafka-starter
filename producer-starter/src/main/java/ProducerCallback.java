import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class ProducerCallback implements Callback {


    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            log.error(exception.getMessage(), exception);
        }else{
            log.info(metadata.toString());
        }
    }
}
