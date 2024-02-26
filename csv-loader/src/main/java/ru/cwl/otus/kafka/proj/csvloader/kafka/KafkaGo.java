package ru.cwl.otus.kafka.proj.csvloader.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.cwl.otus.kafka.proj.csvloader.AppConfig;

@RequiredArgsConstructor
@Component
public class KafkaGo {
    private final KafkaTemplate<Long,String> kafkaTemplate;
    private final AppConfig config;
    public void consumer(String[] a){
        final long trId = Long.parseLong(a[1]);
        kafkaTemplate.send(config.getTopic(),trId,String.join(",",a));
    }
}
