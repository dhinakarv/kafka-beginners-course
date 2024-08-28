package io.kafka.demo.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("hello, I am a kafka consumer");
        String groupId = "my-java-application";
        String topic = "demo_java";
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

        //create consumer properties
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("group.id",groupId);
        props.setProperty("auto.offset.reset", "earliest");
        //none -> we must have consumer group, else it fails. So not recommended
        //earliest --> gets all the messages (--from beginning from cli)
        //latest --> gets only latest messages upon failure

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        try {
            kafkaConsumer.subscribe(Arrays.asList(topic));
            //Poll for messages
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));// might throw wakeup Exception if there are no messages
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key:" + record.key() + ":Value:" + record.value());
                    log.info("Partition:" + record.partition() + ":offset:" + record.offset());

                }
            }
        }catch(WakeupException we){

        }
    }
}
