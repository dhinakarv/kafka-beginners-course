package io.kafka.demo.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
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
        for(int j =0 ; j < 2; j++) {
            //Create Producer Record
            for (int i = 0; i < 4; i++) {
                String topic = "demo_java";
                String key = "Id_" + i;
                //when you create keys, producer make sures that all keys would be send to the same partitioner
                //By default, without keys, kafka sends messages to same partitioner --> sticky partitioner
                //you may add RoundRobinPartitioner in properties.put(_,-) but thats not recommended,
                // instead, we may use keys to send the messages to different partitioner
                String value = "Hello World" + i + 10;
                //Create Producer Record
                ProducerRecord producerRecord = new ProducerRecord(topic, key, value);

                //send data - Asynchronous
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            log.info(
                                    "Topic:" + recordMetadata.topic() +
                                            "Key:" + key +
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
        }
        //flush and close the producer - Synchronous - sends all data from producer and blocks until its done
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
