package app.JavaAOP.controller;


import org.apache.kafka.common.protocol.types.Field;
import org.springframework.stereotype.Service;
import org.springframework.kafka.annotation.KafkaListener;
import io.prometheus.client.Counter;

@Service
public class ConsumerController {
    private final Counter kafkaEventsCounter;

    public ConsumerController(){
        kafkaEventsCounter = Counter.build().name("kafka_events_received_total")
                .help("Total number of Kafka events received").register();
    }

    @KafkaListener(topics = "test", groupId = "metric-consumer-group")
    public void listen(String eventData){
        System.out.println("Received event data: " + eventData);
        kafkaEventsCounter.inc();
    }
}
