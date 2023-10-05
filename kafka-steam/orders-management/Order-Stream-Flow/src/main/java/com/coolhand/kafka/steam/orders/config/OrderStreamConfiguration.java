package com.coolhand.kafka.steam.orders.config;

import com.coolhand.kafka.steam.orders.topology.OrderManagementTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Slf4j
public class OrderStreamConfiguration {

    public NewTopic order(){
        return TopicBuilder.name(OrderManagementTopology.ORDER)
                .partitions(1)
                .replicas(1)
                .build();
    }

    public NewTopic orders(){
        return TopicBuilder.name(OrderManagementTopology.ORDERS)
                .partitions(1)
                .replicas(1)
                .build();
    }

    public NewTopic generalOrder(){
        return TopicBuilder.name(OrderManagementTopology.GENERAL_ORDER)
                .partitions(1)
                .replicas(1)
                .build();
    }

    public NewTopic generalOrderCount(){
        return TopicBuilder.name(OrderManagementTopology.GENERAL_ORDER_COUNT)
                .partitions(1)
                .replicas(1)
                .build();
    }

    public NewTopic generalOrderCountWindow(){
        return TopicBuilder.name(OrderManagementTopology.GENERAL_ORDER_COUNT_WINDOWS)
                .partitions(1)
                .replicas(1)
                .build();
    }

    public NewTopic generalOrderRevenueWindow(){
        return TopicBuilder.name(OrderManagementTopology.GENERAL_ORDER_REVENUE_WINDOWS)
                .partitions(1)
                .replicas(1)
                .build();
    }

    public NewTopic restaurantOrder(){
        return TopicBuilder.name(OrderManagementTopology.RESTAURANT_ORDER)
                .partitions(1)
                .replicas(1)
                .build();
    }


    public NewTopic restaurantOrderCount(){
        return TopicBuilder.name(OrderManagementTopology.RESTAURANT_ORDER_COUNT)
                .partitions(1)
                .replicas(1)
                .build();
    }


    public NewTopic restaurantOrderRevenue(){
        return TopicBuilder.name(OrderManagementTopology.RESTAURANT_ORDER_REVENUE)
                .partitions(1)
                .replicas(1)
                .build();
    }


    public NewTopic restaurantOrderCountWindow(){
        return TopicBuilder.name(OrderManagementTopology.RESTAURANT_ORDER_COUNT_WINDOWS)
                .partitions(1)
                .replicas(1)
                .build();
    }


    public NewTopic restaurantOrderRevenueWindow(){
        return TopicBuilder.name(OrderManagementTopology.RESTAURANT_ORDER_REVENUE_WINDOWS)
                .partitions(1)
                .replicas(1)
                .build();
    }



}
