package com.logicbig.example;

import java.util.Arrays;

public class Running4Consumers {
    public static void main(String[] args) throws Exception {
        String[] consumerGroups = new String[4];
        Arrays.fill(consumerGroups, "test-consumer-group");
        ConsumerGroupExample.run(4, consumerGroups);
    }
}