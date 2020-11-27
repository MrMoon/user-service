package com.apollo.demo.user;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

@SpringBootApplication
public class UserApplication {

    public static void main(String[] args) {
        SpringApplication.run(UserApplication.class , args);
    }

    @Bean
    NewTopic userTopic() {
        return new NewTopic("user" , 3 , (short) 3);
    }

    /*@Bean
    Function<KStream<String , String>, KStream<String, String>> reduceUser() {
        return s -> s
                .groupByKey()
                .reduce((s1 , v1) -> v1 , Materialized.as("users")).toStream();
    }*/
}
