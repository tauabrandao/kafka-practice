package com.brandao.kafkapractice.demos;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args){

        String bootstrapServers = "localhost:9092";
        String topic = "first-topic";
        String message = "Hello, this is another message to the topic";

        //CREATION OF PRODUCER PROPERTIES
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //CREATION OF THE PRODUCER
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //CREATION OF PRODUCER RECORD
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

        //SEND DATA
        producer.send(record);

        producer.flush();
        producer.close();

    }


}
