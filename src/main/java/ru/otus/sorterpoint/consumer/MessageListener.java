//package ru.otus.sorterpoint.consumer;
//
//import avro.schema.SmartPhoneAvro;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Component;
//
//import java.util.UUID;
//
//@Component
//@Slf4j
//public class MessageListener {
//
//    @KafkaListener(topics = "reception-point-topic", groupId = "test-consumer-group")
//    public void mskConsumer(ConsumerRecord<UUID, SmartPhoneAvro> consumerRecord) {
//        log.info("SmartPhoneAvro: {}", consumerRecord.value().getSsn());
//    }
//}
