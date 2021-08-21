package com.brandao.kafkapractice.demos;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args){

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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

        for(int i=0; i<100; i++){
            //CREATION OF PRODUCER RECORD
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message +" - " +i);

            //SEND DATA
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is trown

                    if(Optional.ofNullable(e).isPresent())
                        logger.error(String.format("Error while producing : %s", e.getMessage()));


                    logger.info(
                            new StringBuilder()
                                    .append("\nNEW METADATA \n")
                                    .append(String.format("TOPIC : %s\n", recordMetadata.topic()))
                                    .append(String.format("PARTITION : %d\n", recordMetadata.partition()))
                                    .append(String.format("OFFSET : %d\n", recordMetadata.offset()))
                                    .append(String.format("TIMESTAMP : %d\n", recordMetadata.timestamp()))
                                    .toString()
                    );


                }
            });
        }



        producer.flush();
        producer.close();

    }


}
