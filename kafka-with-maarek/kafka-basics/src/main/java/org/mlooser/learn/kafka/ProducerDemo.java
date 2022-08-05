package org.mlooser.learn.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; ++i) {
            String key = "key_" + (i % 3);
            String message = "message_" + i;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("demo_java",key, message);

            producer.send(record,(metadata, exception) -> {
                if(exception == null){
                   log.info(
                           "Key: " + record.key()
                           + " Partition: " + metadata.partition()
                           + " Offset: " + metadata.offset()
                   );
                }
                else{
                    log.error("Exception while sending record.",exception);
                }
            });
        }





        producer.flush();
        producer.close();
    }
}
