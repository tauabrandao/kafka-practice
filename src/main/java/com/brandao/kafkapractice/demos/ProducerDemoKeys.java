package com.brandao.kafkapractice.demos;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    //SE USARMOS UMA CHAVE (KEY) GARANTIMOS QUE TODAS AS CHAVES SEMPRE IRÃO PARA A MESMA PARTIÇÃO

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "localhost:9092";
        String topic = "new-topic";
        String message = "Hello, this is another message to the topic";

        //CREATION OF PRODUCER PROPERTIES
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //CREATION OF THE PRODUCER
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i<10; i++){

            String key = "id_" +Integer.toString(i);
            logger.info("key " +key);

            //CREATION OF PRODUCER RECORD
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message +" - " +i);

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
            }).get(); //block the .send() to make it synchronous 0 don't do this in production!
        }



        producer.flush();
        producer.close();

    }


}
