package org.mlooser.learn.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("demo_java"));

            registerConsumerCleanup(consumer);

            try {
                pollMessages(consumer);
            } catch (WakeupException we) {
                log.info("Wakeup from consumer");
            }
        }
    }

    private static void pollMessages(KafkaConsumer<String, String> consumer) {
        while (true) {
            log.info("Polling...");
            ConsumerRecords<String, String> recodrs = consumer.poll(Duration.ofSeconds(1));
            handlePolledRecords(recodrs);
        }
    }

    private static void handlePolledRecords(ConsumerRecords<String, String> recodrs) {
        for (var record : recodrs) {
            log.info(
                    "Topic: " + record.topic()
                            + " Key: " + record.key()
                            + " Partition: " + record.partition()
                            + " Offset: " + record.offset()
                            + " Message: " + record.value()
            );
        }
    }

    private static void registerConsumerCleanup(KafkaConsumer<String, String> consumer) {
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }
}
