package com.coolhand.kafka.steam.greetingspringboot.domain;

import java.time.LocalDateTime;

public record Greeting(String message,
                       LocalDateTime timeStamp) {
}
