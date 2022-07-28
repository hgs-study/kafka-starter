package com.example.springkafkaproducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@Slf4j
@SpringBootApplication
public class SpringKafkaProducerApplication implements CommandLineRunner {

	private static String TOPIC_NAME = "test";
	private final KafkaTemplate<String,String> customKafkaTemplate;

	public SpringKafkaProducerApplication(KafkaTemplate<String, String> customKafkaTemplate) {
		this.customKafkaTemplate = customKafkaTemplate;
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaProducerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//        for (int i = 0; i < 10; i++) {
//			System.out.println(i);
//			template.send(TOPIC_NAME, "test" + i);
//        }

		//비동기로 브로커에 적재 여부 확인
		// callback으로 확인 가능
		// 성공 -> onSuccess , 실패 -> onFailure
		ListenableFuture<SendResult<String, String>> future = customKafkaTemplate.send(TOPIC_NAME, "test");

		future.addCallback(new KafkaSendCallback<String, String>(){

			@Override
			public void onSuccess(SendResult<String, String> result) {
				log.info("success");
			}

			@Override
			public void onFailure(KafkaProducerException ex) {
				log.info("fail");
			}
		});

		Thread.sleep(1000);
	}
}
