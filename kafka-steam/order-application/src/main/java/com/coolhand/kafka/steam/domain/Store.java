package com.coolhand.kafka.steam.domain;

public record Store(String locationId,
                    Address address,
                    String contactNum) {
    public Store() {
        this("",new Address() ,"");
    }
}
