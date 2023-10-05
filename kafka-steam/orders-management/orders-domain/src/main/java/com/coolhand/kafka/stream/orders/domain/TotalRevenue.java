package com.coolhand.kafka.stream.orders.domain;



import java.math.BigDecimal;

public record TotalRevenue(String locationId,
                           Integer runnuingOrderCount,
                           BigDecimal runningRevenue) {

    public TotalRevenue() {
        this("",0,new BigDecimal(0.0));
    }

    public TotalRevenue updateRunningRevenue(String key, Order order) {

        var newOrdersCount = this.runnuingOrderCount+1;
        var newRevenue = this.runningRevenue.add(order.finalAmount());
        return new TotalRevenue(key, newOrdersCount, newRevenue);

    }
}
