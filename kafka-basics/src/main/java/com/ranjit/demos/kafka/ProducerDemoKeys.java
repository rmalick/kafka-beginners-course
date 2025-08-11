package com.ranjit.demos.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerDemoKeys {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Hello, Kafka Producer with callback Demo!");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            //create a producer record

            String topic = "demo_topic";
            for (int j = 0; j < 2; j++) {
                logger.info("Key\t   Partition \t message");
                for (int i = 0; i < 5; i++) {
                    //send data
                    String key = "id_" + i;
                    String value = "Hello world " + i;
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception == null) {
                            // the record was successfully sent
                            logger.info("{}\t{}\t{}", key, metadata.partition(), producerRecord.value());
                        } else {
                            // an error occurred
                            logger.error("Error while producing", exception);
                        }
                    });
                }

                Thread.sleep(5000);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // flush and close the producer

    }

}