package com.coolhand.kafka.customers;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomerProfile {
    private String name;
    private int amount;
    private String time;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "CustomerProfile{" +
                "name='" + name + '\'' +
                ", amount=" + amount +
                ", time=" + Instant.now().toString() +
                '}';
    }
}
