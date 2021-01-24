package com.cruelengine.kafka;

import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        System.out.println("Hello World");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        try {

            // //Create producer
            for (int i = 0; i < 10; i++) {
                String topic = "first_topic";
                String value = "hello world " + Integer.toString(i);
                String key = "id_" + Integer.toString(i);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                logger.info("Key " + key);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // Executes everytime record is sent or exception is thrown
                        if (exception == null) {
                            logger.info("Received new metatdata \n" + "Topic: " + metadata.topic() + "\n"
                                    + "Partition: " + metadata.partition() + "\n" + "Offset" + metadata.offset() + "\n"
                                    + "Timestamp :" + metadata.timestamp());
                        } else {
                            logger.error("Error while producing" + exception);
                        }
                    }

                }).get();

            }
        } catch (Exception e) {
            logger.error("Exception : ", e);
        } finally {
            producer.flush();

            producer.close();
        }
    }

}
