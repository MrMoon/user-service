package com.apollo.demo.user;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.internals.DefaultKafkaSender;
import reactor.kafka.sender.internals.ProducerFactory;

import java.util.Collections;
import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Bean
    KafkaReceiver kafkaReceiver() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG , "user-client");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG , "user-group");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG , true);

        return new DefaultKafkaReceiver(ConsumerFactory.INSTANCE
                , ReceiverOptions.create(properties).subscription(Collections.singleton("user")));
    }

    @Bean
    KafkaSender<String , String> kafkaSender() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class);
        properties.put(ProducerConfig.RETRIES_CONFIG , 10);
        return new DefaultKafkaSender<>(ProducerFactory.INSTANCE , SenderOptions.create(properties));
    }

}
