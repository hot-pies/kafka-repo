package com.coolhand.kafka.customers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Date;

public class CustomerInfo {
    public static void main(String[] args)  {
        // Create a new ObjectMapper
        ObjectMapper objectMapper = new ObjectMapper();

        CustomerProfile customer = new CustomerProfile();
        customer.setName("Vivek");
        customer.setAmount(999);
        customer.setTime(Instant.now().toString());

        try {
            String json=objectMapper.writeValueAsString(customer);
            System.out.println("json object is : \n"+json);
        }catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }
}
