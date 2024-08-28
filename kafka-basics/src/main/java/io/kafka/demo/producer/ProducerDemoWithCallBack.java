package io.kafka.demo.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());
    public static void main(String[] args) {
        log.info("hello");

        //create producer properties

        //Connect to local producer
        //Properties props = new Properties();
        //props.put("bootstrap.servers","127.0.0.1:9092");

        Properties props = new Properties();
        //Playground - using free provided by upstash
        props.put("bootstrap.servers", "https://national-donkey-7849-us1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"bmF0aW9uYWwtZG9ua2V5LTc4NDkkyWhC123PKQLOUWOQ_0YdvDbg6qZP5HnaDtw\" " +
                "password=\"MDQzYmVhMzQtOTEzNS00YzE3LWE1YjEtZGUzYWM2MGE1NWFj\";");

        //Producer Properties
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //create producer
        KafkaProducer<String, String> kafkaProducer =new KafkaProducer<>(props);

        for(int i = 0; i < 10 ; i++) {
            //Create Producer Record
            ProducerRecord producerRecord = new ProducerRecord("demo_java", "Hello World - Kafka" + i);


            //send data - Asynchronous
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Received New MetaData:\n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "TimeStamp:" + recordMetadata.timestamp() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Partition:" + recordMetadata.partition());
                    } else {
                        log.error("Error while Producing:", e);
                    }
                }
            });
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //flush and close the producer - Synchronous - sends all data from producer and blocks until its done
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
