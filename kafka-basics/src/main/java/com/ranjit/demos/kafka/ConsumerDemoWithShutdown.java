package com.ranjit.demos.kafka;


import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerDemoWithShutdown {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Hello, Kafka Consumer Demo!");

        String groupId = "my-java-application";
        String topic = "demo_topic";
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            // this will throw an exception
            // we can catch it and exit the application gracefully
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (Exception e) {
                logger.error("Error during shutdown: ", e);
            }
        }));

        try {
            consumer.subscribe(Collections.singleton(topic));
            while (true) {
//                logger.info("polling for new data...");
                // poll for new data
                var records = consumer.poll(Duration.ofSeconds(5));
                records.forEach(record -> {
                    logger.info("Key: {}, Value: {}, Partition: {}, Offset: {}",
                            record.key(),
                            record.value(),
                            record.partition(),
                            record.offset());
                });
            }
        } catch (WakeupException e) {
            // this is expected on shutdown
            logger.info("Received shutdown signal, exiting...");
        } catch (Exception e) {
            logger.error("Unexpected error: ", e);
        } finally {
            consumer.close();
            logger.info("Consumer closed, exiting application.");
        }

    }

}