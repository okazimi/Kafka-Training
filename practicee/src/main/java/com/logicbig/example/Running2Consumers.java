package com.logicbig.example;

import java.util.Arrays;

public class Running2Consumers {
    public static void main(String[] args) throws Exception {
        String[] consumerGroups = new String[2];
        Arrays.fill(consumerGroups, "test-consumer-group");
        ConsumerGroupExample.run(2, consumerGroups);
    }
}