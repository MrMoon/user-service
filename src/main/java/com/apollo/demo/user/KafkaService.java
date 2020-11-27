package com.apollo.demo.user;

import lombok.RequiredArgsConstructor;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.ConnectableFlux;
import reactor.kafka.receiver.KafkaReceiver;

import javax.annotation.PostConstruct;

@Service
@RequiredArgsConstructor
public class KafkaService {

    private final KafkaReceiver<String , String> kafkaReceiver;
    private ConnectableFlux<ServerSentEvent<String>> eventConnectableFlux;

    @PostConstruct
    public void init() {
        eventConnectableFlux = kafkaReceiver
                .receive()
                .map(stringStringReceiverRecord ->
                        ServerSentEvent.builder(stringStringReceiverRecord.value()).build())
                .publish();
        eventConnectableFlux.connect();
    }

    public ConnectableFlux<ServerSentEvent<String>> getEventConnectableFlux() {
        return eventConnectableFlux;
    }

}
