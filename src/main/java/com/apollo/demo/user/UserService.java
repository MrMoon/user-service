package com.apollo.demo.user;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.util.Optional;

public interface UserService {

    Mono<Optional<User>> saveUserEvent(@NotNull User user);

}
