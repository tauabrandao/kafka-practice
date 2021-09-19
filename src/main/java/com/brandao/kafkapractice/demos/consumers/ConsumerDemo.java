package com.brandao.kafkapractice.demos.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootstrapServers = "localhost:9092";
        String groupId = "my-application";
        String newTopic = "new-topic";
        String firstTopic = "first-topic";


        //CREATE CONSUMER CONFIGS
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //CREATE CONSUMER
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //SUBSCRIPE CONSUMER TO OUR TOPIC(S)
        //consumer.subscribe(Arrays.asList(newTopic));

        //é possível subscrever para mais de um tópico
        consumer.subscribe(Arrays.asList(newTopic, firstTopic));

        while(true){ //apenas para exemplo - não fazer isso em produção.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                logger.info("key: " +record.key() + ", value: " +record.value());
                logger.info("partition: " +record.partition() +", offset: " +record.offset());
            });
        }



    }
}
