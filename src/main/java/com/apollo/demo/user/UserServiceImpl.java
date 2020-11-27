package com.apollo.demo.user;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.apachecommons.CommonsLog;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Optional;
import java.util.Properties;

@Service
@CommonsLog(topic = "User Service Logger")
public class UserServiceImpl implements UserService {

    private final KafkaSender<String , String> userEventSender;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public UserServiceImpl() {
        final Properties userEventSenderProperties = new Properties();
        userEventSenderProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , "localhost:9092");
        userEventSenderProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class);
        userEventSenderProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class);

        userEventSender = KafkaSender.create(SenderOptions.create(userEventSenderProperties));
    }

    @Override
    public Mono<Optional<User>> saveUserEvent(@NotNull User user) {
        user.setId("id");
        return userEventSender.send(Mono.just(SenderRecord.create(new ProducerRecord<>("user" , userToBinary(user)) , 1)))
                .next()
                .doOnNext(log::info)
                .map(integerSenderResult -> integerSenderResult.recordMetadata() != null && integerSenderResult.exception() == null ? Optional.of(user) : Optional.empty());
    }

    private String userToBinary(User user) {
        try {
            return objectMapper.writeValueAsString(user);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
