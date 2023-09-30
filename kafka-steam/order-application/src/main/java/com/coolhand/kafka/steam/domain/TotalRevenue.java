package com.coolhand.kafka.steam.domain;

import java.math.BigDecimal;

public record TotalRevenue(String locationId,
                           Integer runningOrderCount,
                           BigDecimal runningRevenue) {
    public TotalRevenue(){
        this("",0,BigDecimal.valueOf(0.0));
    }

    public TotalRevenue updateRunningRevenue(String key,Order order){
        var newOrderCount=this.runningOrderCount+1;
        var newRevenue=this.runningRevenue.add(order.finalAmount());
        return new TotalRevenue(key,newOrderCount,newRevenue);
    }

}
