package com.coolhand.kafka.steam.domain;

import java.time.LocalDateTime;

public record Greeting(String message,
                       LocalDateTime timeStamp){}