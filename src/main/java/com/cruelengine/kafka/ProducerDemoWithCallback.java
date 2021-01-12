package com.cruelengine.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
         System.out.println("Hello World");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // //Create producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // //create producer record
        ProducerRecord<String ,String> record =
                new ProducerRecord<>("first_topic","hello world");
        // //send data

        producer.send(record, new Callback(){

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				//Executes everytime record is sent or exception is thrown
                if (exception == null) {
                    logger.info("Received new metatdata \n" + "Topic: "
                     + metadata.topic() + "\n" +"Partition: " + metadata.partition() 
                     + "\n" + "Offset" + metadata.offset() + "\n" + "Timestamp :" + metadata.timestamp());
                }else{
                    logger.error("Error while producing" + exception);
                }
			}
            
        });

        producer.flush();

        producer.close();
    }
    
}
