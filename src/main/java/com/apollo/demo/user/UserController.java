package com.apollo.demo.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.apachecommons.CommonsLog;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

@RestController
@RequestMapping(value = "/user")
@CommonsLog(topic = "UserController")
@RequiredArgsConstructor
@CrossOrigin(origins = "http://localhost:4200")
public class UserController {

    private final UserService userService;
    private final KafkaService kafkaService;

    @PostMapping(value = "/create")
    public Mono<ResponseEntity<User>> createUser(@RequestBody User user) {
        return this.userService.saveUserEvent(user).map(userEvent -> userEvent.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_ACCEPTABLE).build()));
    }

    @GetMapping(value = "/stream/{id}" , produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<User>> userStream(@PathVariable("id") String id) {
        return kafkaService
                .getEventConnectableFlux()
                .doOnNext(stringServerSentEvent -> log.info(stringServerSentEvent.data()))
                .map(stringServerSentEvent -> {
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.setPropertyNamingStrategy(new PropertyNamingStrategy.KebabCaseStrategy());

                    User user;
                    try {
                        user = objectMapper.readValue(stringServerSentEvent.data() , User.class);
                    } catch (Exception e) {
                        log.error(e.getMessage());
                        return null;
                    }
                    return ServerSentEvent.builder(user).build();
                })
                .filter(Objects::nonNull)
                .filter(userServerSentEvent -> Objects.requireNonNull(userServerSentEvent.data()).getId().equals(id))
                .onErrorContinue((throwable , o) -> log.error(throwable.getMessage()));

    }

}
